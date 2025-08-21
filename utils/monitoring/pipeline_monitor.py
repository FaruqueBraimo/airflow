"""
Comprehensive monitoring and error handling for the financial statement pipeline.
"""
import logging
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum
import threading
from pathlib import Path

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class MetricType(Enum):
    """Types of metrics to track."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


@dataclass
class PipelineAlert:
    """Represents a pipeline alert."""
    timestamp: datetime
    level: AlertLevel
    component: str
    message: str
    details: Dict[str, Any]
    resolved: bool = False


@dataclass
class PipelineMetric:
    """Represents a pipeline metric."""
    name: str
    type: MetricType
    value: float
    timestamp: datetime
    tags: Dict[str, str]


class PipelineMetrics:
    """
    Collects and manages pipeline metrics.
    """
    
    def __init__(self):
        """Initialize metrics collection."""
        self._metrics: List[PipelineMetric] = []
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._timers: Dict[str, List[float]] = {}
        self._lock = threading.Lock()
    
    def increment_counter(self, name: str, value: float = 1.0, tags: Optional[Dict[str, str]] = None):
        """Increment a counter metric."""
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + value
            
            metric = PipelineMetric(
                name=name,
                type=MetricType.COUNTER,
                value=self._counters[name],
                timestamp=datetime.utcnow(),
                tags=tags or {}
            )
            self._metrics.append(metric)
    
    def set_gauge(self, name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """Set a gauge metric."""
        with self._lock:
            self._gauges[name] = value
            
            metric = PipelineMetric(
                name=name,
                type=MetricType.GAUGE,
                value=value,
                timestamp=datetime.utcnow(),
                tags=tags or {}
            )
            self._metrics.append(metric)
    
    def record_timer(self, name: str, duration: float, tags: Optional[Dict[str, str]] = None):
        """Record a timer metric."""
        with self._lock:
            if name not in self._timers:
                self._timers[name] = []
            self._timers[name].append(duration)
            
            metric = PipelineMetric(
                name=name,
                type=MetricType.TIMER,
                value=duration,
                timestamp=datetime.utcnow(),
                tags=tags or {}
            )
            self._metrics.append(metric)
    
    def get_counter(self, name: str) -> float:
        """Get current counter value."""
        return self._counters.get(name, 0.0)
    
    def get_gauge(self, name: str) -> float:
        """Get current gauge value."""
        return self._gauges.get(name, 0.0)
    
    def get_timer_stats(self, name: str) -> Dict[str, float]:
        """Get timer statistics."""
        if name not in self._timers or not self._timers[name]:
            return {}
        
        values = self._timers[name]
        return {
            'count': len(values),
            'sum': sum(values),
            'min': min(values),
            'max': max(values),
            'avg': sum(values) / len(values)
        }
    
    def get_recent_metrics(self, minutes: int = 60) -> List[PipelineMetric]:
        """Get metrics from the last N minutes."""
        cutoff = datetime.utcnow() - timedelta(minutes=minutes)
        return [m for m in self._metrics if m.timestamp >= cutoff]
    
    def export_metrics(self) -> Dict[str, Any]:
        """Export all metrics for external monitoring systems."""
        return {
            'counters': self._counters.copy(),
            'gauges': self._gauges.copy(),
            'timer_stats': {name: self.get_timer_stats(name) for name in self._timers},
            'export_timestamp': datetime.utcnow().isoformat()
        }


class PipelineAlertManager:
    """
    Manages alerts and notifications for pipeline issues.
    """
    
    def __init__(self, max_alerts: int = 1000):
        """Initialize alert manager."""
        self._alerts: List[PipelineAlert] = []
        self._max_alerts = max_alerts
        self._lock = threading.Lock()
        self._alert_handlers = {
            AlertLevel.INFO: self._log_info,
            AlertLevel.WARNING: self._log_warning,
            AlertLevel.ERROR: self._log_error,
            AlertLevel.CRITICAL: self._log_critical
        }
    
    def create_alert(self, level: AlertLevel, component: str, message: str, details: Optional[Dict] = None):
        """Create a new alert."""
        alert = PipelineAlert(
            timestamp=datetime.utcnow(),
            level=level,
            component=component,
            message=message,
            details=details or {}
        )
        
        with self._lock:
            self._alerts.append(alert)
            
            # Maintain alert limit
            if len(self._alerts) > self._max_alerts:
                self._alerts = self._alerts[-self._max_alerts:]
        
        # Handle alert
        self._handle_alert(alert)
    
    def _handle_alert(self, alert: PipelineAlert):
        """Handle an alert based on its level."""
        handler = self._alert_handlers.get(alert.level, self._log_info)
        handler(alert)
    
    def _log_info(self, alert: PipelineAlert):
        """Handle info level alerts."""
        logger.info(f"[{alert.component}] {alert.message}")
    
    def _log_warning(self, alert: PipelineAlert):
        """Handle warning level alerts."""
        logger.warning(f"[{alert.component}] {alert.message}")
    
    def _log_error(self, alert: PipelineAlert):
        """Handle error level alerts."""
        logger.error(f"[{alert.component}] {alert.message}")
    
    def _log_critical(self, alert: PipelineAlert):
        """Handle critical level alerts."""
        logger.critical(f"[{alert.component}] {alert.message}")
        # TODO: Implement additional critical alert handling
        # - Send immediate notifications
        # - Create incident tickets
        # - Trigger emergency procedures
    
    def get_recent_alerts(self, level: Optional[AlertLevel] = None, hours: int = 24) -> List[PipelineAlert]:
        """Get recent alerts, optionally filtered by level."""
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        alerts = [a for a in self._alerts if a.timestamp >= cutoff]
        
        if level:
            alerts = [a for a in alerts if a.level == level]
        
        return alerts
    
    def get_alert_summary(self) -> Dict[str, int]:
        """Get summary of alert counts by level."""
        summary = {level.value: 0 for level in AlertLevel}
        
        for alert in self._alerts:
            summary[alert.level.value] += 1
        
        return summary


class PipelineHealthChecker:
    """
    Monitors pipeline health and performance.
    """
    
    def __init__(self, metrics: PipelineMetrics, alerts: PipelineAlertManager):
        """Initialize health checker."""
        self.metrics = metrics
        self.alerts = alerts
        self.health_thresholds = {
            'kafka_lag_seconds': 300,  # 5 minutes
            'processing_time_seconds': 600,  # 10 minutes
            'error_rate_percent': 5.0,
            'queue_depth': 1000,
            'memory_usage_percent': 85.0,
            'disk_usage_percent': 90.0
        }
    
    def check_kafka_health(self, last_message_time: datetime) -> bool:
        """Check Kafka consumer health."""
        try:
            lag_seconds = (datetime.utcnow() - last_message_time).total_seconds()
            self.metrics.set_gauge('kafka_lag_seconds', lag_seconds)
            
            if lag_seconds > self.health_thresholds['kafka_lag_seconds']:
                self.alerts.create_alert(
                    AlertLevel.WARNING,
                    'kafka_consumer',
                    f'Kafka consumer lag: {lag_seconds:.1f}s',
                    {'lag_seconds': lag_seconds, 'threshold': self.health_thresholds['kafka_lag_seconds']}
                )
                return False
            
            return True
            
        except Exception as e:
            self.alerts.create_alert(
                AlertLevel.ERROR,
                'health_checker',
                f'Error checking Kafka health: {str(e)}'
            )
            return False
    
    def check_processing_performance(self, batch_size: int, processing_duration: float) -> bool:
        """Check processing performance."""
        try:
            per_message_time = processing_duration / batch_size if batch_size > 0 else 0
            throughput = batch_size / processing_duration if processing_duration > 0 else 0
            
            self.metrics.set_gauge('processing_time_per_message', per_message_time)
            self.metrics.set_gauge('throughput_messages_per_second', throughput)
            
            if processing_duration > self.health_thresholds['processing_time_seconds']:
                self.alerts.create_alert(
                    AlertLevel.WARNING,
                    'performance',
                    f'Slow processing detected: {processing_duration:.1f}s for {batch_size} messages',
                    {'duration': processing_duration, 'batch_size': batch_size, 'per_message': per_message_time}
                )
                return False
            
            return True
            
        except Exception as e:
            self.alerts.create_alert(
                AlertLevel.ERROR,
                'health_checker',
                f'Error checking processing performance: {str(e)}'
            )
            return False
    
    def check_error_rates(self, total_processed: int, total_errors: int) -> bool:
        """Check error rates."""
        try:
            if total_processed == 0:
                return True
                
            error_rate = (total_errors / total_processed) * 100
            self.metrics.set_gauge('error_rate_percent', error_rate)
            
            if error_rate > self.health_thresholds['error_rate_percent']:
                self.alerts.create_alert(
                    AlertLevel.ERROR,
                    'error_tracking',
                    f'High error rate: {error_rate:.1f}%',
                    {'error_rate': error_rate, 'total_processed': total_processed, 'total_errors': total_errors}
                )
                return False
            
            return True
            
        except Exception as e:
            self.alerts.create_alert(
                AlertLevel.ERROR,
                'health_checker',
                f'Error checking error rates: {str(e)}'
            )
            return False
    
    def get_overall_health_status(self) -> Dict[str, Any]:
        """Get overall health status."""
        try:
            recent_alerts = self.alerts.get_recent_alerts(hours=1)
            critical_alerts = [a for a in recent_alerts if a.level == AlertLevel.CRITICAL]
            error_alerts = [a for a in recent_alerts if a.level == AlertLevel.ERROR]
            warning_alerts = [a for a in recent_alerts if a.level == AlertLevel.WARNING]
            
            if critical_alerts:
                health_status = "critical"
            elif error_alerts:
                health_status = "error"
            elif warning_alerts:
                health_status = "warning"
            else:
                health_status = "healthy"
            
            return {
                'status': health_status,
                'timestamp': datetime.utcnow().isoformat(),
                'recent_alerts': {
                    'critical': len(critical_alerts),
                    'error': len(error_alerts),
                    'warning': len(warning_alerts),
                    'info': len([a for a in recent_alerts if a.level == AlertLevel.INFO])
                },
                'key_metrics': {
                    'kafka_lag_seconds': self.metrics.get_gauge('kafka_lag_seconds'),
                    'error_rate_percent': self.metrics.get_gauge('error_rate_percent'),
                    'throughput_mps': self.metrics.get_gauge('throughput_messages_per_second')
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting health status: {str(e)}")
            return {
                'status': 'unknown',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }


class PipelineMonitor:
    """
    Main monitoring coordinator for the financial statement pipeline.
    """
    
    def __init__(self):
        """Initialize pipeline monitor."""
        self.metrics = PipelineMetrics()
        self.alerts = PipelineAlertManager()
        self.health_checker = PipelineHealthChecker(self.metrics, self.alerts)
        self.start_time = datetime.utcnow()
    
    def track_kafka_consumption(self, message_count: int, duration: float, errors: int = 0):
        """Track Kafka consumption metrics."""
        self.metrics.increment_counter('messages_consumed_total', message_count)
        self.metrics.increment_counter('kafka_errors_total', errors)
        self.metrics.record_timer('kafka_consumption_duration', duration)
        
        if errors > 0:
            self.alerts.create_alert(
                AlertLevel.WARNING,
                'kafka_consumer',
                f'Kafka consumption errors: {errors} out of {message_count} messages',
                {'errors': errors, 'message_count': message_count}
            )
    
    def track_validation(self, total_messages: int, valid_messages: int, invalid_messages: int, duration: float):
        """Track validation metrics."""
        self.metrics.increment_counter('messages_validated_total', total_messages)
        self.metrics.increment_counter('messages_valid_total', valid_messages)
        self.metrics.increment_counter('messages_invalid_total', invalid_messages)
        self.metrics.record_timer('validation_duration', duration)
        
        validation_rate = (valid_messages / total_messages * 100) if total_messages > 0 else 0
        self.metrics.set_gauge('validation_success_rate', validation_rate)
        
        if invalid_messages > 0:
            self.alerts.create_alert(
                AlertLevel.WARNING,
                'data_validation',
                f'Data validation issues: {invalid_messages} invalid out of {total_messages} messages',
                {'invalid': invalid_messages, 'total': total_messages, 'rate': validation_rate}
            )
    
    def track_pdf_generation(self, total_pdfs: int, successful_pdfs: int, failed_pdfs: int, duration: float, total_size: int):
        """Track PDF generation metrics."""
        self.metrics.increment_counter('pdfs_generated_total', total_pdfs)
        self.metrics.increment_counter('pdfs_successful_total', successful_pdfs)
        self.metrics.increment_counter('pdfs_failed_total', failed_pdfs)
        self.metrics.record_timer('pdf_generation_duration', duration)
        self.metrics.set_gauge('pdf_total_size_bytes', total_size)
        
        success_rate = (successful_pdfs / total_pdfs * 100) if total_pdfs > 0 else 0
        self.metrics.set_gauge('pdf_success_rate', success_rate)
        
        if failed_pdfs > 0:
            self.alerts.create_alert(
                AlertLevel.ERROR if success_rate < 80 else AlertLevel.WARNING,
                'pdf_generation',
                f'PDF generation failures: {failed_pdfs} out of {total_pdfs} PDFs failed',
                {'failed': failed_pdfs, 'total': total_pdfs, 'success_rate': success_rate}
            )
    
    def track_pipeline_execution(self, execution_id: str, duration: float, success: bool, summary: Dict):
        """Track overall pipeline execution."""
        self.metrics.increment_counter('pipeline_executions_total')
        self.metrics.record_timer('pipeline_execution_duration', duration)
        
        if success:
            self.metrics.increment_counter('pipeline_executions_successful')
            self.alerts.create_alert(
                AlertLevel.INFO,
                'pipeline_execution',
                f'Pipeline execution {execution_id} completed successfully',
                summary
            )
        else:
            self.metrics.increment_counter('pipeline_executions_failed')
            self.alerts.create_alert(
                AlertLevel.ERROR,
                'pipeline_execution',
                f'Pipeline execution {execution_id} failed',
                summary
            )
        
        # Calculate success rate
        total_executions = self.metrics.get_counter('pipeline_executions_total')
        successful_executions = self.metrics.get_counter('pipeline_executions_successful')
        success_rate = (successful_executions / total_executions * 100) if total_executions > 0 else 0
        self.metrics.set_gauge('pipeline_success_rate', success_rate)
    
    def get_monitoring_dashboard_data(self) -> Dict[str, Any]:
        """Get data for monitoring dashboard."""
        uptime = (datetime.utcnow() - self.start_time).total_seconds()
        
        return {
            'pipeline_info': {
                'uptime_seconds': uptime,
                'start_time': self.start_time.isoformat(),
                'current_time': datetime.utcnow().isoformat()
            },
            'health_status': self.health_checker.get_overall_health_status(),
            'metrics_summary': self.metrics.export_metrics(),
            'recent_alerts': [
                {
                    'timestamp': alert.timestamp.isoformat(),
                    'level': alert.level.value,
                    'component': alert.component,
                    'message': alert.message
                }
                for alert in self.alerts.get_recent_alerts(hours=24)
            ]
        }
    
    def export_monitoring_data(self, output_path: Path):
        """Export monitoring data to file."""
        try:
            data = self.get_monitoring_dashboard_data()
            
            with open(output_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            
            logger.info(f"Monitoring data exported to {output_path}")
            
        except Exception as e:
            logger.error(f"Error exporting monitoring data: {str(e)}")


# Global monitor instance
pipeline_monitor = PipelineMonitor()