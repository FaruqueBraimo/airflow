# Use official Airflow image as base
FROM apache/airflow:2.8.0-python3.10

# Switch to root to install system dependencies
USER root

# Install system dependencies for WeasyPrint and PDF generation
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        python3-dev \
        python3-pip \
        python3-cffi \
        python3-brotli \
        libpango-1.0-0 \
        libpangoft2-1.0-0 \
        libffi-dev \
        libjpeg-dev \
        libopenjp2-7-dev \
        libxml2-dev \
        libxslt1-dev \
        zlib1g-dev \
        pkg-config \
        libcairo2-dev \
        libpangocairo-1.0-0 \
        libgdk-pixbuf2.0-dev \
        shared-mime-info \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy the application code
COPY --chown=airflow:root ./dags /opt/airflow/dags
COPY --chown=airflow:root ./utils /opt/airflow/utils
COPY --chown=airflow:root ./config /opt/airflow/config
COPY --chown=airflow:root ./templates /opt/airflow/templates

# Create necessary directories
RUN mkdir -p /opt/airflow/logs \
    && mkdir -p /opt/airflow/output \
    && mkdir -p /opt/airflow/plugins

# Set Python path
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

# Set default command
CMD ["webserver"]