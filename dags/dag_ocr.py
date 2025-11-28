from multiprocessing import context
import tempfile
from typing import Dict, List
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
# Use decorators from airflow.decorators for compatibility (sdk is newer but may not be available)
try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task
from airflow.models import Variable
from azure.storage.blob import BlobServiceClient

from datetime import datetime, timedelta
import os
import pytesseract
from PIL import Image
from pdf2image import convert_from_path
import json
import traceback
import logging

# Configure logging - Use WARNING level in production for better performance
# Set to INFO or DEBUG for troubleshooting
LOG_LEVEL = logging.WARNING  # Change to logging.INFO for more verbose logs
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_n_past': False
}

ALLOWED_FILE_TYPES = ['.pdf', '.png', '.jpg', '.jpeg', '.tiff', '.bmp']


def perform_ocr(**context):
    """
    Perform OCR on the uploaded document
    """
    logger.info("Starting OCR task")
    
    try:
        # Get file path from XCom (passed from API trigger)
        ti = context['ti']
        file_path = ti.xcom_pull(key='file_path')
        file_name = ti.xcom_pull(key='file_name')
        
        logger.info(f"Received file_path: {file_path}, file_name: {file_name}")

        if not file_path:
            error_msg = "No file path provided in XCom"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        if not file_name:
            error_msg = "No file name provided in XCom"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Validate file exists
        if not os.path.exists(file_path):
            error_msg = f"File does not exist: {file_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        file_size = os.path.getsize(file_path)
        logger.info(f"File size: {file_size} bytes")

        output_dir = os.path.expanduser('~/airflow-output')
        logger.info(f"Creating output directory: {output_dir}")
        try:
            os.makedirs(output_dir, exist_ok=True)
        except OSError as e:
            error_msg = f"Failed to create output directory {output_dir}: {str(e)}"
            logger.error(error_msg)
            raise

        # Determine file type and process accordingly
        file_ext = os.path.splitext(file_name)[1].lower()
        logger.info(f"Processing file with extension: {file_ext}")

        extracted_text = None
        
        try:
            if file_ext == '.pdf':
                logger.info("Processing PDF file")
                try:
                    images = convert_from_path(file_path)
                    logger.info(f"PDF converted to {len(images)} images")
                    all_text = []
                    for i, image in enumerate(images):
                        logger.debug(f"Processing page {i+1}/{len(images)}")
                        try:
                            text = pytesseract.image_to_string(image)
                            all_text.append(f"--- Page {i+1} ---\n{text}\n")
                        except Exception as e:
                            logger.warning(f"Failed to process page {i+1}: {str(e)}")
                            all_text.append(f"--- Page {i+1} ---\n[OCR Error: {str(e)}]\n")
                    extracted_text = "\n".join(all_text)
                    logger.info(f"Extracted {len(extracted_text)} characters from PDF")
                except Exception as e:
                    error_msg = f"Failed to convert PDF to images: {str(e)}"
                    logger.error(error_msg)
                    raise
                    
            elif file_ext in ['.png', '.jpg', '.jpeg', '.tiff', '.bmp']:
                logger.info(f"Processing image file: {file_ext}")
                try:
                    image = Image.open(file_path)
                    logger.info(f"Image opened successfully. Size: {image.size}, Mode: {image.mode}")
                    extracted_text = pytesseract.image_to_string(image)
                    logger.info(f"Extracted {len(extracted_text)} characters from image")
                except Exception as e:
                    error_msg = f"Failed to process image: {str(e)}"
                    logger.error(error_msg)
                    raise
            else:
                error_msg = f"Unsupported file type: {file_ext}. Allowed types: {ALLOWED_FILE_TYPES}"
                logger.error(error_msg)
                raise ValueError(error_msg)

            if not extracted_text or len(extracted_text.strip()) == 0:
                logger.warning("No text extracted from the document")

            # Save output to file
            output_file = os.path.join(
                output_dir, f"{os.path.splitext(file_name)[0]}_ocr_output.txt")
            logger.info(f"Saving OCR output to: {output_file}")
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(extracted_text)
                logger.info(f"Successfully saved output file: {output_file}")
            except Exception as e:
                error_msg = f"Failed to write output file {output_file}: {str(e)}"
                logger.error(error_msg)
                raise

            # Also save metadata
            metadata = {
                'original_file': file_name,
                'output_file': output_file,
                'timestamp': datetime.now().isoformat(),
                'text_length': len(extracted_text),
                'file_size_bytes': file_size,
                'file_extension': file_ext
            }

            metadata_file = os.path.join(
                output_dir, f"{os.path.splitext(file_name)[0]}_metadata.json")
            logger.info(f"Saving metadata to: {metadata_file}")
            try:
                with open(metadata_file, 'w') as f:
                    json.dump(metadata, f, indent=2)
                logger.info(f"Successfully saved metadata file: {metadata_file}")
            except Exception as e:
                error_msg = f"Failed to write metadata file {metadata_file}: {str(e)}"
                logger.error(error_msg)
                raise

            logger.info(f"OCR completed successfully. Output saved to: {output_file}")
            return {
                'status': 'success',
                'output_file': output_file,
                'metadata_file': metadata_file,
                'text_length': len(extracted_text)
            }

        except Exception as e:
            error_msg = f"OCR processing failed: {str(e)}"
            error_stacktrace = traceback.format_exc()
            logger.error(error_msg)
            logger.error(f"Stacktrace: {error_stacktrace}")
            
            # Return error details instead of raising exception
            return {
                'status': 'failed',
                'output_file': None,
                'metadata_file': None,
                'text_length': 0,
                'error': error_msg,
                'error_stacktrace': error_stacktrace
            }
    except Exception as e:
        error_msg = f"OCR task failed with unexpected error: {str(e)}"
        error_stacktrace = traceback.format_exc()
        logger.error(error_msg)
        logger.error(f"Stacktrace: {error_stacktrace}")
        raise
    finally:
        # Clean up uploaded file
        if 'file_path' in locals() and file_path and os.path.exists(file_path):
            try:
                logger.info(f"Cleaning up temporary file: {file_path}")
                os.remove(file_path)
                logger.info("Temporary file removed successfully")
            except Exception as e:
                logger.warning(f"Failed to remove temporary file {file_path}: {str(e)}")


with DAG(
    dag_id='document-ocr-v4',
    default_args=default_args,
    description="DAG for performing OCR on uploaded documents",
    schedule=None,
    start_date=datetime(2025, 11, 10, 3, 0, 0),
    catchup=False,
    tags=['ocr', 'document-processing']
) as document_ocr_dag:

    ocr_task = PythonOperator(
        task_id='perform_ocr_task',
        python_callable=perform_ocr,
    )


@dag(
    dag_id='document_ocr_folder_v15',
    default_args=default_args,
    description="DAG for performing OCR on all documents in a folder",
    schedule=None,
    start_date=datetime(2025, 11, 11, 3, 0, 0),
    catchup=False,
    tags= ['ocr', 'document-processing', 'folder'],
    max_active_runs=10,
    # Performance optimizations
    max_active_tasks=50  # Allow more concurrent tasks
)
def document_ocr_folder():
    @task
    def get_conf(**context) -> Dict[str, str]:
        """
        Read dag_run.conf and return validated configuration.
        Required keys:
        - container: Azure Blob Storage container name
        - folder_path: path/prefix within the container (e.g. 'incoming/batch1/')
        Optional:
        - allowed_extensions: list of extensions to include
        """
        logger.info("Reading DAG run configuration")
        try:
            # Access dag_run.conf directly through context
            dag_run = context.get('dag_run')
            if not dag_run:
                error_msg = "dag_run not found in context"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            conf = dag_run.conf if dag_run.conf else {}
            logger.info(f"Retrieved configuration: {list(conf.keys())}")
            
            if not conf:
                error_msg = "dag_run.conf is empty"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            if 'container' not in conf:
                error_msg = "dag_run.conf missing required key 'container'"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            if 'folder_path' not in conf:
                error_msg = "dag_run.conf missing required key 'folder_path'"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            container = conf['container']
            folder_path = conf['folder_path']
            logger.info(f"Configuration validated - Container: {container}, Folder: {folder_path}")
            
            # Validate allowed_extensions if provided
            if 'allowed_extensions' in conf:
                allowed_ext = conf['allowed_extensions']
                if not isinstance(allowed_ext, list):
                    logger.warning(f"allowed_extensions should be a list, got {type(allowed_ext)}. Using default.")
                    conf['allowed_extensions'] = ALLOWED_FILE_TYPES
                else:
                    logger.info(f"Using custom allowed extensions: {allowed_ext}")
            else:
                logger.info(f"Using default allowed extensions: {ALLOWED_FILE_TYPES}")
            
            return conf
        except Exception as e:
            error_msg = f"Failed to get configuration: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            raise

    @task
    def list_blobs(conf: Dict[str, str]) -> List[str]:
        logger.info("Starting blob listing task")
        blob_names: List[str] = []
        
        try:
            container = conf['container']
            prefix = conf['folder_path']
            if not prefix.endswith('/'):
                prefix = prefix + '/'
            
            logger.info(f"Listing blobs in container '{container}' with prefix '{prefix}'")
            
            # Get connection string
            try:
                connection_string = Variable.get('AZURE_BLOB_CONNECTION_STRING')
                logger.info("Successfully retrieved Azure Blob connection string")
            except Exception as e:
                error_msg = f"Failed to get AZURE_BLOB_CONNECTION_STRING variable: {str(e)}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Get allowed extensions
            allowed_extensions = conf.get('allowed_extensions', ALLOWED_FILE_TYPES)
            logger.info(f"Filtering files with extensions: {allowed_extensions}")

            try:
                blob_service_client = BlobServiceClient.from_connection_string(
                    connection_string)
                logger.info("Created BlobServiceClient")
            except Exception as e:
                error_msg = f"Failed to create BlobServiceClient: {str(e)}"
                logger.error(error_msg)
                raise
            
            try:
                container_client = blob_service_client.get_container_client(container)
                logger.info(f"Created container client for '{container}'")
            except Exception as e:
                error_msg = f"Failed to get container client for '{container}': {str(e)}"
                logger.error(error_msg)
                raise

            # List blobs
            try:
                blob_count = 0
                skipped_count = 0
                for blob in container_client.list_blobs(name_starts_with=prefix):
                    blob_count += 1
                    name = blob.name
                    
                    # Skip directories
                    if name.endswith('/'):
                        logger.debug(f"Skipping directory: {name}")
                        skipped_count += 1
                        continue
                    
                    try:
                        # Split the filename and get extension
                        name_parts = os.path.splitext(name)
                        ext = name_parts[1].lower() if len(name_parts) > 1 else ''
                        
                        if ext in allowed_extensions:
                            blob_names.append(name)
                            logger.debug(f"Added blob to processing list: {name}")
                        else:
                            logger.debug(f"Skipping blob with unsupported extension '{ext}': {name}")
                            skipped_count += 1
                    except (IndexError, AttributeError) as e:
                        logger.warning(f"Could not process blob name '{name}': {e}")
                        skipped_count += 1
                        continue
                
                logger.info(f"Blob listing complete. Found {len(blob_names)} files to process, "
                           f"{skipped_count} skipped, {blob_count} total blobs examined")
                
            except Exception as e:
                error_msg = f"Failed to list blobs: {str(e)}"
                logger.error(error_msg)
                raise
            
            if not blob_names:
                error_msg = f"No input files found under '{prefix}' in container '{container}'. " \
                           f"Total blobs examined: {blob_count}, Skipped: {skipped_count}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            logger.info(f"Successfully listed {len(blob_names)} blobs for processing")
            return blob_names
            
        except Exception as e:
            error_msg = f"Blob listing task failed: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            raise

    @task(max_active_tis_per_dag=20)  # Increased parallelism for better performance
    def process_blob(blob_name: str, conf: Dict[str, str]) -> Dict[str, str]:
        """
        Download the blob to a temp file, run OCR, and upload results under 'output/'.
        """
        logger.info(f"Starting processing for blob: {blob_name}")
        local_path = None
        
        try:
            # Get connection string
            try:
                connection_string = Variable.get('AZURE_BLOB_CONNECTION_STRING')
                logger.debug("Retrieved Azure Blob connection string")
            except Exception as e:
                error_msg = f"Failed to get AZURE_BLOB_CONNECTION_STRING variable: {str(e)}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            container = conf['container']
            logger.info(f"Processing blob in container: {container}")

            try:
                # Create blob clients (reused for download and upload operations)
                blob_service_client = BlobServiceClient.from_connection_string(
                    connection_string)
                container_client = blob_service_client.get_container_client(container)
                # Reduced logging for performance - only log on error
            except Exception as e:
                error_msg = f"Failed to create blob clients: {str(e)}"
                logger.error(error_msg)
                raise

            basename = os.path.basename(blob_name)
            file_ext = os.path.splitext(basename)[1].lower()
            logger.info(f"Blob basename: {basename}, extension: {file_ext}")

            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = os.path.join(tmpdir, basename)
                
                # Download blob - optimized for performance
                logger.info(f"Downloading blob '{blob_name}'")
                try:
                    with open(local_path, 'wb') as f:
                        stream = container_client.download_blob(blob_name)
                        blob_data = stream.readall()
                        f.write(blob_data)
                    file_size = len(blob_data)
                    logger.info(f"Downloaded {file_size} bytes")
                except Exception as e:
                    error_msg = f"Failed to download blob '{blob_name}': {str(e)}"
                    logger.error(error_msg)
                    raise

                # Validate file exists and has content
                if not os.path.exists(local_path):
                    error_msg = f"Downloaded file does not exist: {local_path}"
                    logger.error(error_msg)
                    raise FileNotFoundError(error_msg)
                
                actual_size = os.path.getsize(local_path)
                if actual_size == 0:
                    error_msg = f"Downloaded file is empty: {local_path}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                # Perform OCR - optimized logging
                extracted_text = None
                logger.info(f"Starting OCR for {file_ext} file")
                
                try:
                    if file_ext == '.pdf':
                        try:
                            images = convert_from_path(local_path)
                            logger.info(f"PDF: {len(images)} pages")
                            all_text = []
                            for i, image in enumerate(images):
                                try:
                                    text = pytesseract.image_to_string(image)
                                    all_text.append(f"--- Page {i+1} ---\n{text}\n")
                                except Exception as e:
                                    logger.warning(f"Page {i+1} OCR failed: {str(e)}")
                                    all_text.append(f"--- Page {i+1} ---\n[OCR Error: {str(e)}]\n")
                            extracted_text = "\n".join(all_text)
                            logger.info(f"PDF OCR complete: {len(extracted_text)} chars")
                        except Exception as e:
                            error_msg = f"PDF conversion failed: {str(e)}"
                            logger.error(error_msg)
                            raise
                            
                    elif file_ext in ['.png', '.jpg', '.jpeg', '.tiff', '.bmp']:
                        try:
                            image = Image.open(local_path)
                            extracted_text = pytesseract.image_to_string(image)
                            logger.info(f"Image OCR complete: {len(extracted_text)} chars")
                        except Exception as e:
                            error_msg = f"Image processing failed: {str(e)}"
                            logger.error(error_msg)
                            raise
                    else:
                        error_msg = f"Unsupported file type: {file_ext}"
                        logger.error(error_msg)
                        raise ValueError(error_msg)
                    
                    if not extracted_text or len(extracted_text.strip()) == 0:
                        logger.warning("No text extracted")
                    
                except Exception as e:
                    error_msg = f"OCR processing failed: {str(e)}"
                    logger.error(error_msg)
                    raise

                # Prepare output paths
                root_prefix = conf['folder_path'].rstrip('/')
                output_prefix = f"{root_prefix}/output2"
                output_text_blob = f"{output_prefix}/{os.path.splitext(basename)[0]}_ocr_output.txt"
                output_meta_blob = f"{output_prefix}/{os.path.splitext(basename)[0]}_metadata.json"

                # Upload OCR results - optimized
                try:
                    text_bytes = extracted_text.encode('utf-8')
                    container_client.upload_blob(
                        name=output_text_blob, data=text_bytes, overwrite=True)
                    logger.info(f"Uploaded OCR output: {output_text_blob}")
                except Exception as e:
                    error_msg = f"Failed to upload OCR output: {str(e)}"
                    logger.error(error_msg)
                    raise

                # Create and upload metadata
                metadata = {
                    'original_blob': blob_name,
                    'output_blob': output_text_blob,
                    'timestamp': datetime.now().isoformat(),
                    'text_length': len(extracted_text),
                    'file_size_bytes': file_size,
                    'file_extension': file_ext
                }
                
                try:
                    metadata_json = json.dumps(metadata, indent=2)
                    container_client.upload_blob(
                        name=output_meta_blob, data=metadata_json, overwrite=True)
                    logger.info(f"Uploaded metadata: {output_meta_blob}")
                except Exception as e:
                    error_msg = f"Failed to upload metadata: {str(e)}"
                    logger.error(error_msg)
                    raise

                logger.info(f"Completed '{blob_name}': {len(extracted_text)} chars extracted")
                return {
                    'status': 'success',
                    'original_blob': blob_name,
                    'output_blob': output_text_blob,
                    'metadata_blob': output_meta_blob,
                    'text_length': len(extracted_text),
                    'error': None,
                    'error_stacktrace': None
                }
                
        except Exception as e:
            error_msg = f"OCR processing failed for blob '{blob_name}': {str(e)}"
            error_stacktrace = traceback.format_exc()
            logger.error(error_msg)
            logger.error(f"Stacktrace: {error_stacktrace}")
            return {
                'status': 'failed',
                'original_blob': blob_name,
                'output_blob': None,
                'metadata_blob': None,
                'text_length': 0,
                'error': error_msg,
                'error_stacktrace': error_stacktrace
            }
        finally:
            # Clean up local file if it exists (tempdir handles this, but explicit cleanup for safety)
            if local_path and os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except Exception as e:
                    logger.warning(f"Cleanup failed for {local_path}: {str(e)}")

    conf = get_conf()
    blob_list = list_blobs(conf)
    # Use partial to bind conf parameter, then expand blob_name
    process_results = process_blob.partial(conf=conf).expand(blob_name=blob_list)

# Instantiate the DAG by calling the function
document_ocr_folder()
