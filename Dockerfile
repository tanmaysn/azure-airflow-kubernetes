FROM apache/airflow:3.1.2

USER root

# Install system dependencies for OCR
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    tesseract-ocr-eng \
    libtesseract-dev \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

# Create output directory
RUN mkdir -p /root/airflow-output && chmod 777 /root/airflow-output

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip 
RUN pip install --no-cache-dir -r /requirements.txt