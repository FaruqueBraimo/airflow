# Financial Statement Processing Pipeline

A comprehensive Apache Airflow-based pipeline for processing financial statement data and generating professional PDF reports. The system supports both Kafka streaming and file-based input sources with dynamic template selection and robust error handling.

## 🏗️ Architecture Overview

The pipeline consists of the following key components:

- **Data Ingestion**: Kafka consumer or file watcher for input data
- **Data Validation**: Pydantic-based validation with business rule checks
- **Template Management**: Dynamic template selection with versioning
- **PDF Generation**: Professional PDF reports using WeasyPrint and ReportLab
- **Monitoring**: Comprehensive metrics and alerting system

## 📋 Features

- ✅ **Dual Data Sources**: Support for both Kafka streaming and file-based input
- ✅ **Template Versioning**: Dynamic template selection based on metadata
- ✅ **Professional PDFs**: Multiple statement formats (monthly, quarterly, annual)
- ✅ **Data Validation**: Comprehensive validation with error handling
- ✅ **Monitoring**: Built-in metrics, alerts, and health checks
- ✅ **Scalability**: Designed for high-volume processing
- ✅ **Docker Support**: Complete containerized deployment

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Apache Airflow 2.8.0+

### Installation

1. **Clone the repository**:
```bash
git clone <repository-url>
cd financial-statement-pipeline
```

2. **Install dependencies**:
```bash
pip install -r requirements.txt
```

3. **Set up environment**:
```bash
python config/environment_setup.py
```

4. **Start with Docker Compose**:
```bash
docker-compose up -d
```

### Configuration

The system can be configured through environment variables:

#### Data Source Selection
```bash
# Use file watcher (default)
export DATA_SOURCE=file_watcher

# Or use Kafka
export DATA_SOURCE=kafka
```

#### File Watcher Configuration
```bash
export INPUT_DIR=./input
export ARCHIVE_DIR=./archive
export ERROR_DIR=./error
export FILE_BATCH_SIZE=50
```

#### Kafka Configuration
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=financial-statements
export KAFKA_GROUP_ID=airflow-statement-processor
```

## 📁 Project Structure

```
financial-statement-pipeline/
├── dags/
│   └── financial_statement_dag.py      # Main Airflow DAG
├── utils/
│   ├── kafka/                          # Kafka consumer utilities
│   ├── file_watcher/                   # File watching utilities
│   ├── data_sources/                   # Data source factory
│   ├── validation/                     # Data validation
│   ├── templates/                      # Template management
│   ├── pdf/                           # PDF generation
│   └── monitoring/                     # Monitoring utilities
├── templates/
│   ├── monthly/                        # Monthly statement templates
│   ├── quarterly/                      # Quarterly statement templates
│   └── annual/                         # Annual statement templates
├── config/
│   ├── airflow_config.py              # Configuration settings
│   └── environment_setup.py           # Environment setup
├── input/                              # Input directory for JSON files
├── archive/                            # Processed files archive
├── error/                              # Error files directory
├── output/                             # Generated PDF output
├── logs/                              # Application logs
├── tests/                             # Test cases
├── docker-compose.yml                 # Docker services
├── Dockerfile                         # Application container
├── requirements.txt                   # Python dependencies
└── README.md                          # This file
```

## 🔄 Data Flow

1. **Data Ingestion**
   - File Watcher: Monitors `input/` directory for JSON files
   - Kafka Consumer: Consumes from configured Kafka topic

2. **Validation & Transformation**
   - JSON schema validation
   - Business rule validation
   - Data normalization and enrichment

3. **Template Selection**
   - Dynamic selection based on metadata
   - Version-aware template loading
   - Template caching for performance

4. **PDF Generation**
   - HTML template rendering with Jinja2
   - Professional PDF generation with WeasyPrint
   - File validation and archiving

5. **Monitoring & Alerting**
   - Real-time metrics collection
   - Health status monitoring
   - Alert generation for failures

## 📊 Data Format

### Input JSON Schema

```json
{
  "statement_id": "STMT-2024-001",
  "customer_id": "CUST-12345", 
  "statement_date": "2024-01-31T23:59:59Z",
  "statement_type": "monthly",
  "customer_info": {
    "customer_id": "CUST-12345",
    "name": "John Doe",
    "address": {...},
    "email": "john.doe@email.com"
  },
  "transactions": [...],
  "balances": {...},
  "totals": {...},
  "metadata": {
    "template_name": "monthly",
    "template_version": "1.0",
    "currency": "USD"
  }
}
```

## 🖥️ User Interfaces

### Airflow Web UI
- **URL**: http://localhost:8080
- **Credentials**: admin/admin
- **Features**: DAG management, execution monitoring, task logs

### Grafana Dashboard  
- **URL**: http://localhost:3000
- **Credentials**: admin/admin
- **Features**: Metrics visualization, alert management

### Prometheus Metrics
- **URL**: http://localhost:9090
- **Features**: Raw metrics, query interface

## 📝 Usage Examples

### Using File Watcher

1. **Place JSON file in input directory**:
```bash
cp sample_statement.json input/
```

2. **Monitor processing**:
```bash
# Check Airflow UI for DAG execution
# Processed files move to archive/
# Generated PDFs appear in output/
```

### Using Kafka

1. **Send message to Kafka topic**:
```bash
kafka-console-producer --broker-list localhost:9092 --topic financial-statements < sample_statement.json
```

2. **Monitor in Airflow UI**:
```bash
# DAG will automatically trigger on schedule
# Check task logs for processing details
```

### Switching Data Sources

Using Airflow Variables:
```bash
# Set via Airflow UI: Admin -> Variables
# Key: data_source_type
# Value: kafka or file_watcher
```

Using Environment Variables:
```bash
export DATA_SOURCE=kafka
docker-compose restart
```

## 🔧 Development

### Running Tests
```bash
pytest tests/
```

### Code Quality
```bash
black .
flake8 .
mypy .
```

### Adding New Templates

1. Create template directory:
```bash
mkdir templates/custom/
```

2. Add template files:
```bash
templates/custom/
├── template.html
└── config.json
```

3. Update template configuration in template manager.

## 📈 Monitoring

### Key Metrics
- **Messages Processed**: Total count of processed statements
- **Processing Time**: Average time per statement  
- **Success Rate**: Percentage of successful PDF generations
- **Error Rate**: Percentage of validation/processing errors
- **Queue Size**: Current backlog of pending messages

### Alerting
- Email notifications for critical errors
- Slack integration for warnings
- Grafana dashboard alerts
- Log-based monitoring

### Health Checks
- Data source connectivity
- Processing performance
- Error rate thresholds
- Resource utilization

## 🛠️ Configuration Reference

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_SOURCE` | `file_watcher` | Data source type (kafka/file_watcher) |
| `INPUT_DIR` | `./input` | File watcher input directory |
| `ARCHIVE_DIR` | `./archive` | Processed files archive |
| `ERROR_DIR` | `./error` | Error files directory |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka server addresses |
| `KAFKA_TOPIC` | `financial-statements` | Kafka topic name |
| `LOG_LEVEL` | `INFO` | Application log level |

### Airflow Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `data_source_type` | `file_watcher` | Override data source |
| `kafka_batch_size` | `50` | Kafka batch size |
| `file_batch_size` | `50` | File processing batch size |

## 🚨 Troubleshooting

### Common Issues

**File watcher not processing files**:
- Check input directory permissions
- Verify JSON file format
- Check error directory for failed files

**Kafka connection errors**:
- Verify Kafka server is running
- Check network connectivity
- Validate topic configuration

**PDF generation failures**:
- Check template syntax
- Verify WeasyPrint dependencies
- Review error logs in Airflow UI

**Template not found errors**:
- Verify template directory structure
- Check metadata template_name field
- Confirm template file permissions

### Log Locations

- **Airflow Logs**: Available in Airflow UI
- **Application Logs**: `logs/pipeline.log`
- **Error Files**: `error/` directory with `.error.txt` files

### Performance Tuning

- Adjust batch sizes based on memory constraints
- Configure appropriate polling intervals
- Monitor resource utilization
- Scale worker nodes as needed

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📞 Support

For support and questions:
- Create an issue in the repository
- Check the troubleshooting section
- Review Airflow and application logs

## 🔄 Version History

- **v1.0.0**: Initial release with Kafka and file watcher support
- **v1.1.0**: Enhanced monitoring and alerting
- **v1.2.0**: Additional template formats and validation improvements