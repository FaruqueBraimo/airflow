"""
Setup script for the Financial Statement Processing Pipeline.
"""
from setuptools import setup, find_packages
from pathlib import Path

# Read README file
readme_path = Path(__file__).parent / "README.md"
long_description = readme_path.read_text() if readme_path.exists() else ""

setup(
    name="financial-statement-pipeline",
    version="1.0.0",
    description="Apache Airflow pipeline for processing financial statements from Kafka and generating PDF reports",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Data Engineering Team",
    author_email="data-engineering@company.com",
    python_requires=">=3.8",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "apache-airflow==2.8.0",
        "apache-airflow-providers-apache-kafka==1.3.0", 
        "kafka-python==2.0.2",
        "reportlab==4.0.7",
        "weasyprint==60.2",
        "pydantic==2.5.2",
        "jinja2==3.1.2",
        "pandas==2.1.4",
        "jsonschema==4.20.0",
        "python-dateutil==2.8.2",
        "requests==2.31.0"
    ],
    extras_require={
        "dev": [
            "pytest==7.4.3",
            "pytest-mock==3.12.0",
            "pytest-cov==4.1.0",
            "black==23.12.1",
            "flake8==6.1.0",
            "mypy==1.8.0"
        ],
        "monitoring": [
            "prometheus-client==0.19.0",
            "grafana-api==1.0.3"
        ]
    },
    entry_points={
        "console_scripts": [
            "setup-pipeline=config.environment_setup:setup_environment",
        ]
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Office/Business :: Financial",
    ],
)