from multiprocessing import context
import tempfile
from typing import Dict, List
from airflow import DAG
from airflow.operators.python import PythonOperator
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
     # Get file path from XCom (passed from API trigger)
    ti = context['ti']
    file_path = ti.xcom_pull(key='file_path')
    file_name = ti.xcom_pull(key='file_name')

    if not file_path:
        raise ValueError("No file path provided")

    output_dir = os.path.expanduser('~/airflow-output')
    os.makedirs(output_dir, exist_ok=True)

    # Determine file type and process accordingly
    file_ext = os.path.splitext(file_name)[1].lower()

    try:
        if file_ext == '.pdf':
            # Convert PDF to images
            images = convert_from_path(file_path)
            all_text = []
            for i, image in enumerate(images):
                text = pytesseract.image_to_string(image)
                all_text.append(f"--- Page {i+1} ---\n{text}\n")
            extracted_text = "\n".join(all_text)
        elif file_ext in ['.png', '.jpg', '.jpeg', '.tiff', '.bmp']:
            # Process image directly
            image = Image.open(file_path)
            extracted_text = pytesseract.image_to_string(image)
        else:
            raise ValueError(f"Unsupported file type: {file_ext}")

        # Save output to file
        output_file = os.path.join(
            output_dir, f"{os.path.splitext(file_name)[0]}_ocr_output.txt")
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(extracted_text)

        # Also save metadata
        metadata = {
            'original_file': file_name,
            'output_file': output_file,
            'timestamp': datetime.now().isoformat(),
                'text_length': len(extracted_text)
        }

        metadata_file = os.path.join(
            output_dir, f"{os.path.splitext(file_name)[0]}_metadata.json")
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)

        print(f"OCR completed. Output saved to: {output_file}")
        return {
            'status': 'success',
            'output_file': output_file,
            'metadata_file': metadata_file,
                'text_length': len(extracted_text)
        }

    except Exception as e:
        error_msg = f"OCR failed: {str(e)}"
        error_stacktrace = traceback.format_exc()
        print(error_msg)
        print(f"Stacktrace: {error_stacktrace}")
        
        # Return error details instead of raising exception
        return {
            'status': 'failed',
            'output_file': None,
            'metadata_file': None,
            'text_length': 0,
            'error': error_msg,
            'error_stacktrace': error_stacktrace
        }
    finally:
        # Clean up uploaded file
        if os.path.exists(file_path):
            os.remove(file_path)


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
    dag_id='document_ocr_folder_v12',
    default_args=default_args,
    description="DAG for performing OCR on all documents in a folder",
    schedule=None,
    start_date=datetime(2025, 11, 11, 3, 0, 0),
    catchup=False,
    tags= ['ocr', 'document-processing', 'folder'],
    max_active_runs=10
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
        # Access dag_run.conf directly through context
        dag_run = context.get('dag_run')
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        
        if not conf or 'container' not in conf or 'folder_path' not in conf:
            raise ValueError(
                "dag_run.conf must include 'container' and 'folder_path'")
        return conf

    @task
    def list_blobs(conf: Dict[str, str]) -> List[str]:
        connection_string = Variable.get('AZURE_BLOB_CONNECTION_STRING')
        container = conf['container']
        prefix = conf['folder_path']
        if not prefix.endswith('/'):
            prefix = prefix + '/'

        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string)
        container_client = blob_service_client.get_container_client(container)

        blob_names: List[str] = []
        for blob in container_client.list_blobs(name_starts_with=prefix):
            name = blob.name
            if name.endswith('/'):
                continue
            try:
                # Split the filename and get extension
                name_parts = os.path.splitext(name)
                ext = name_parts[1].lower() if len(name_parts) > 1 else ''
                if ext in ALLOWED_FILE_TYPES:
                    blob_names.append(name)
            except (IndexError, AttributeError) as e:
                print(f"Warning: Could not process blob name '{name}': {e}")
                continue
        if not blob_names:
            raise ValueError(
                f"No input files found under '{prefix}' in container '{container}'")
        return blob_names

    @task(max_active_tis_per_dag=10, )
    def process_blob(blob_name: str, conf: Dict[str, str]) -> Dict[str, str]:
        """
        Download the blob to a temp file, run OCR, and upload results under 'output/'.
        """
        try:
            connection_string = Variable.get('AZURE_BLOB_CONNECTION_STRING')
            container = conf['container']

            blob_service_client = BlobServiceClient.from_connection_string(
                connection_string)
            container_client = blob_service_client.get_container_client(container)

            basename = os.path.basename(blob_name)
            file_ext = os.path.splitext(basename)[1].lower()

            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = os.path.join(tmpdir, basename)
                # Download
                with open(local_path, 'wb') as f:
                    stream = container_client.download_blob(blob_name)
                    f.write(stream.readall())

                # OCR
                if file_ext == '.pdf':
                    images = convert_from_path(local_path)
                    all_text = []
                    for i, image in enumerate(images):
                        text = pytesseract.image_to_string(image)
                        all_text.append(f"--- Page {i+1} ---\n{text}\n")
                    extracted_text = "\n".join(all_text)
                elif file_ext in ['.png', '.jpg', '.jpeg', '.tiff', '.bmp']:
                    image = Image.open(local_path)
                    extracted_text = pytesseract.image_to_string(image)
                else:
                    raise ValueError(f"Unsupported file types: {file_ext}")

                # Prepare output paths
                root_prefix = conf['folder_path'].rstrip('/')
                output_prefix = f"{root_prefix}/output2"
                output_text_blob = f"{output_prefix}/{os.path.splitext(basename)[0]}_ocr_output.txt"
                output_meta_blob = f"{output_prefix}/{os.path.splitext(basename)[0]}_metadata.json"

                # Upload results
                text_bytes = extracted_text.encode('utf-8')
                container_client.upload_blob(
                    name=output_text_blob, data=text_bytes, overwrite=True)

                metadata = {
                    'original_blob': blob_name,
                    'output_blob': output_text_blob,
                    'timestamp': datetime.now().isoformat(),
                        'text_length': len(extracted_text)
                }
                container_client.upload_blob(name=output_meta_blob, data=json.dumps(
                    metadata, indent=2), overwrite=True)

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
            print(error_msg)
            print(f"Stacktrace: {error_stacktrace}")
            return {
                'status': 'failed',
                'original_blob': blob_name,
                'output_blob': None,
                'metadata_blob': None,
                'text_length': 0,
                'error': error_msg,
                'error_stacktrace': error_stacktrace
            }

    conf = get_conf()
    blob_list = list_blobs(conf)
    # Use partial to bind conf parameter, then expand blob_name
    process_results = process_blob.partial(conf=conf).expand(blob_name=blob_list)

# Instantiate the DAG by calling the function
document_ocr_folder()
