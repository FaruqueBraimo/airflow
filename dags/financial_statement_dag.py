"""
Apache Airflow DAG for processing financial statements from Kafka and generating PDF reports.
"""
import logging
from datetime import datetime, timedelta
from pathlib import Path
import sys
import os


# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

# Import our custom modules
from config.airflow_config import (
    DEFAULT_DAG_ARGS, 
    KAFKA_CONFIG, 
    FILE_WATCHER_CONFIG,
    DATA_SOURCE_TYPE,
    MONITORING_CONFIG
)
from utils.data_sources.data_source_factory import DataSourceManager, DataSourceType
from utils.validation.statement_validator import StatementValidator, DataTransformer
from utils.templates.template_manager import TemplateManager
from utils.pdf.pdf_generator import PDFGenerator

logger = logging.getLogger(__name__)

# DAG Configuration
DAG_ID = 'financial_statement_processing'
DESCRIPTION = 'Process financial statements from Kafka and generate PDF reports'

# Default arguments for all tasks
default_args = {
    'owner': DEFAULT_DAG_ARGS['owner'],
    'depends_on_past': DEFAULT_DAG_ARGS['depends_on_past'],
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': DEFAULT_DAG_ARGS['email_on_failure'],
    'email_on_retry': DEFAULT_DAG_ARGS['email_on_retry'],
    'retries': DEFAULT_DAG_ARGS['retries'],
    'retry_delay': timedelta(minutes=DEFAULT_DAG_ARGS['retry_delay_minutes']),
    'catchup': False,
}

# Create DAG
dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description=DESCRIPTION,
    schedule_interval=timedelta(hours=1),  # Run every hour
    max_active_runs=1,
    tags=['financial', 'kafka', 'pdf', 'statements'],
)


def consume_messages(**context) -> list:
    """
    Task to consume messages from configured data source (Kafka or File Watcher).
    
    Args:
        context: Airflow context
        
    Returns:
        list: List of consumed messages
    """
    data_source_manager = None
    
    try:
        # Get data source type from Airflow Variable or use default
        data_source_type = Variable.get('data_source_type', default_var=DATA_SOURCE_TYPE)
        logger.info(f"Starting message consumption using {data_source_type} data source")
        
        # Initialize data source manager
        data_source_manager = DataSourceManager()
        
        # Get configuration based on source type
        if data_source_type.lower() == 'kafka':
            config = KAFKA_CONFIG.copy()
            batch_size = int(Variable.get('kafka_batch_size', default_var=50))
        else:  # file_watcher
            config = FILE_WATCHER_CONFIG.copy()
            batch_size = int(Variable.get('file_batch_size', default_var=50))
        
        # Initialize and connect to data source
        success = data_source_manager.initialize_source(data_source_type, config, auto_connect=True)
        if not success:
            raise AirflowException(f"Failed to initialize {data_source_type} data source")
        
        # Consume batch of messages
        messages = data_source_manager.consume_batch(batch_size=batch_size)
        
        logger.info(f"Consumed {len(messages)} messages from {data_source_type}")
        
        # Push messages and metadata to XCom for next task
        context['task_instance'].xcom_push(
            key='raw_messages', 
            value=messages
        )
        context['task_instance'].xcom_push(
            key='data_source_info',
            value={
                'type': data_source_type,
                'batch_size': batch_size,
                'message_count': len(messages),
                'status': data_source_manager.get_status()
            }
        )
        
        # Also return for logging
        return messages
        
    except Exception as e:
        logger.error(f"Error consuming messages: {str(e)}")
        raise AirflowException(f"Message consumption failed: {str(e)}")
    finally:
        # Ensure data source is properly closed
        if data_source_manager:
            try:
                data_source_manager.close()
            except Exception as e:
                logger.warning(f"Error closing data source: {str(e)}")


def validate_and_transform_data(**context) -> list:
    """
    Task to validate and transform consumed data.
    
    Args:
        context: Airflow context
        
    Returns:
        list: List of validated and transformed statements
    """
    try:
        logger.info("Starting data validation and transformation")
        
        # Get messages from previous task
        raw_messages = context['task_instance'].xcom_pull(
            task_ids='consume_messages',
            key='raw_messages'
        )
        
        # Get data source info
        data_source_info = context['task_instance'].xcom_pull(
            task_ids='consume_messages',
            key='data_source_info'
        )
        
        if not raw_messages:
            data_source_type = data_source_info.get('type', 'unknown') if data_source_info else 'unknown'
            logger.warning(f"No messages received from {data_source_type} data source")
            return []
        
        # Initialize validator and transformer
        validator = StatementValidator()
        transformer = DataTransformer()
        
        validated_statements = []
        
        for i, message in enumerate(raw_messages):
            try:
                # Validate message
                validated_data = validator.validate_statement_message(message)
                
                if validated_data:
                    # Transform and enrich data
                    transformed_data = transformer.normalize_amounts(validated_data)
                    enriched_data = transformer.enrich_statement_data(transformed_data)
                    
                    validated_statements.append(enriched_data)
                    logger.debug(f"Processed message {i+1}: {enriched_data['statement_id']}")
                else:
                    logger.warning(f"Message {i+1} failed validation")
                    
            except Exception as e:
                logger.error(f"Error processing message {i+1}: {str(e)}")
                continue
        
        logger.info(f"Validated and transformed {len(validated_statements)} statements")
        
        # Push validated data to XCom
        context['task_instance'].xcom_push(
            key='validated_statements',
            value=validated_statements
        )
        
        return validated_statements
        
    except Exception as e:
        logger.error(f"Error in data validation: {str(e)}")
        raise AirflowException(f"Data validation failed: {str(e)}")


def select_templates(**context) -> dict:
    """
    Task to select appropriate templates for each statement.
    
    Args:
        context: Airflow context
        
    Returns:
        dict: Template selections for each statement
    """
    try:
        logger.info("Starting template selection")
        
        # Get validated statements from previous task
        validated_statements = context['task_instance'].xcom_pull(
            task_ids='validate_and_transform_data',
            key='validated_statements'
        )
        
        if not validated_statements:
            logger.warning("No validated statements received")
            return {}
        
        # Initialize template manager
        template_manager = TemplateManager()
        
        template_selections = {}
        
        for statement in validated_statements:
            try:
                statement_id = statement['statement_id']
                metadata = statement.get('metadata', {})
                
                # Select template based on metadata
                template = template_manager.select_template(metadata)
                
                if template:
                    template_selections[statement_id] = {
                        'template_name': metadata.get('template_name', 'monthly'),
                        'template_version': metadata.get('template_version', '1.0'),
                        'selected': True
                    }
                    logger.debug(f"Template selected for {statement_id}")
                else:
                    logger.error(f"No template found for statement {statement_id}")
                    template_selections[statement_id] = {'selected': False}
                    
            except Exception as e:
                logger.error(f"Error selecting template for statement: {str(e)}")
                continue
        
        logger.info(f"Template selection completed for {len(template_selections)} statements")
        
        # Push template selections to XCom
        context['task_instance'].xcom_push(
            key='template_selections',
            value=template_selections
        )
        
        return template_selections
        
    except Exception as e:
        logger.error(f"Error in template selection: {str(e)}")
        raise AirflowException(f"Template selection failed: {str(e)}")


def generate_pdf_reports(**context) -> dict:
    """
    Task to generate PDF reports for all validated statements.
    
    Args:
        context: Airflow context
        
    Returns:
        dict: Results of PDF generation
    """
    try:
        logger.info("Starting PDF generation")
        
        # Get data from previous tasks
        validated_statements = context['task_instance'].xcom_pull(
            task_ids='validate_and_transform_data',
            key='validated_statements'
        )
        
        template_selections = context['task_instance'].xcom_pull(
            task_ids='select_templates',
            key='template_selections'
        )
        
        if not validated_statements:
            logger.warning("No validated statements received for PDF generation")
            return {}
        
        # Initialize PDF generator
        pdf_generator = PDFGenerator()
        
        # Generate PDFs
        generation_results = {}
        successful_generations = 0
        
        for statement in validated_statements:
            try:
                statement_id = statement['statement_id']
                
                # Check if template was selected successfully
                if (template_selections and 
                    statement_id in template_selections and 
                    template_selections[statement_id].get('selected', False)):
                    
                    # Generate PDF
                    pdf_path = pdf_generator.generate_statement_pdf(statement)
                    
                    if pdf_path and pdf_generator.validate_pdf_output(pdf_path):
                        generation_results[statement_id] = {
                            'success': True,
                            'pdf_path': str(pdf_path),
                            'file_size': pdf_path.stat().st_size
                        }
                        successful_generations += 1
                        logger.info(f"Successfully generated PDF for {statement_id}")
                    else:
                        generation_results[statement_id] = {
                            'success': False,
                            'error': 'PDF generation or validation failed'
                        }
                        logger.error(f"Failed to generate valid PDF for {statement_id}")
                else:
                    generation_results[statement_id] = {
                        'success': False,
                        'error': 'Template selection failed'
                    }
                    logger.error(f"Skipping PDF generation for {statement_id} - no template")
                    
            except Exception as e:
                generation_results[statement_id] = {
                    'success': False,
                    'error': str(e)
                }
                logger.error(f"Error generating PDF for {statement_id}: {str(e)}")
                continue
        
        logger.info(f"PDF generation completed: {successful_generations} successful, "
                   f"{len(generation_results) - successful_generations} failed")
        
        # Push results to XCom
        context['task_instance'].xcom_push(
            key='pdf_generation_results',
            value=generation_results
        )
        
        return generation_results
        
    except Exception as e:
        logger.error(f"Error in PDF generation: {str(e)}")
        raise AirflowException(f"PDF generation failed: {str(e)}")


def publish_results_and_cleanup(**context) -> dict:
    """
    Task to publish results and perform cleanup.
    
    Args:
        context: Airflow context
        
    Returns:
        dict: Summary of pipeline execution
    """
    try:
        logger.info("Starting result publishing and cleanup")
        
        # Get results from previous tasks
        pdf_results = context['task_instance'].xcom_pull(
            task_ids='generate_pdf_reports',
            key='pdf_generation_results'
        )
        
        if not pdf_results:
            logger.warning("No PDF generation results received")
            return {}
        
        # Calculate summary statistics
        total_statements = len(pdf_results)
        successful_pdfs = sum(1 for r in pdf_results.values() if r.get('success', False))
        failed_pdfs = total_statements - successful_pdfs
        
        # Calculate total file size
        total_size = sum(
            r.get('file_size', 0) for r in pdf_results.values() 
            if r.get('success', False)
        )
        
        summary = {
            'execution_date': context['execution_date'].isoformat(),
            'total_statements_processed': total_statements,
            'successful_pdf_generations': successful_pdfs,
            'failed_pdf_generations': failed_pdfs,
            'total_pdf_size_bytes': total_size,
            'success_rate': (successful_pdfs / total_statements * 100) if total_statements > 0 else 0,
            'generated_pdfs': [
                r['pdf_path'] for r in pdf_results.values() 
                if r.get('success', False) and 'pdf_path' in r
            ]
        }
        
        logger.info(f"Pipeline execution summary: {summary}")
        
        # TODO: Implement additional result publishing
        # - Send notifications
        # - Upload PDFs to cloud storage
        # - Update database records
        # - Send metrics to monitoring system
        
        return summary
        
    except Exception as e:
        logger.error(f"Error in result publishing: {str(e)}")
        raise AirflowException(f"Result publishing failed: {str(e)}")


def check_pipeline_health(**context):
    """
    Task to check overall pipeline health and send alerts if needed.
    
    Args:
        context: Airflow context
    """
    try:
        # Get execution summary
        summary = context['task_instance'].xcom_pull(
            task_ids='publish_results_and_cleanup'
        )
        
        if summary:
            success_rate = summary.get('success_rate', 0)
            
            # Define health thresholds
            healthy_threshold = 95.0
            warning_threshold = 80.0
            
            if success_rate >= healthy_threshold:
                logger.info(f"Pipeline healthy: {success_rate:.1f}% success rate")
            elif success_rate >= warning_threshold:
                logger.warning(f"Pipeline degraded: {success_rate:.1f}% success rate")
                # TODO: Send warning alerts
            else:
                logger.error(f"Pipeline unhealthy: {success_rate:.1f}% success rate")
                # TODO: Send critical alerts
                
        else:
            logger.error("No execution summary available for health check")
            
    except Exception as e:
        logger.error(f"Error in pipeline health check: {str(e)}")


# Define task dependencies
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

consume_task = PythonOperator(
    task_id='consume_messages',
    python_callable=consume_messages,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_and_transform_data',
    python_callable=validate_and_transform_data,
    dag=dag
)

template_task = PythonOperator(
    task_id='select_templates',
    python_callable=select_templates,
    dag=dag
)

pdf_task = PythonOperator(
    task_id='generate_pdf_reports',
    python_callable=generate_pdf_reports,
    dag=dag
)

publish_task = PythonOperator(
    task_id='publish_results_and_cleanup',
    python_callable=publish_results_and_cleanup,
    dag=dag
)

health_check_task = PythonOperator(
    task_id='check_pipeline_health',
    python_callable=check_pipeline_health,
    dag=dag
)

end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag
)

# Set task dependencies
start_task >> consume_task >> validate_task >> template_task >> pdf_task >> publish_task >> health_check_task >> end_task