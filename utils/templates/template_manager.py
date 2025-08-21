"""
Template management system for financial statement PDF generation.
"""
import json
import logging
from pathlib import Path
from typing import Dict, Optional, List, Any
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, Template, TemplateNotFound
from config.airflow_config import TEMPLATE_CONFIG, TEMPLATES_DIR

logger = logging.getLogger(__name__)


class TemplateVersion:
    """Represents a specific version of a template."""
    
    def __init__(self, name: str, version: str, template_path: Path, config: Dict):
        self.name = name
        self.version = version
        self.template_path = template_path
        self.config = config
        self.created_date = datetime.fromisoformat(config.get('created_date', datetime.utcnow().isoformat()))
        self.is_active = config.get('is_active', True)


class TemplateManager:
    """
    Manages financial statement templates with versioning support.
    """
    
    def __init__(self, templates_dir: Optional[Path] = None):
        """
        Initialize template manager.
        
        Args:
            templates_dir: Path to templates directory
        """
        self.templates_dir = Path(templates_dir or TEMPLATES_DIR)
        self.config = TEMPLATE_CONFIG
        self._template_cache = {}
        self._template_registry = {}
        self._load_templates()
        
        # Configure Jinja2 environment
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(self.templates_dir)),
            autoescape=True,
            trim_blocks=True,
            lstrip_blocks=True
        )
        
        # Add custom filters
        self._register_custom_filters()
    
    def _load_templates(self):
        """Load all available templates from the templates directory."""
        try:
            if not self.templates_dir.exists():
                logger.warning(f"Templates directory not found: {self.templates_dir}")
                return
            
            # Scan for template directories
            for template_type_dir in self.templates_dir.iterdir():
                if template_type_dir.is_dir():
                    self._load_template_type(template_type_dir.name, template_type_dir)
                    
            logger.info(f"Loaded {len(self._template_registry)} template types")
            
        except Exception as e:
            logger.error(f"Error loading templates: {str(e)}")
    
    def _load_template_type(self, template_type: str, type_dir: Path):
        """
        Load all versions of a specific template type.
        
        Args:
            template_type: Type of template (monthly, quarterly, annual)
            type_dir: Directory containing template versions
        """
        try:
            self._template_registry[template_type] = {}
            
            # Look for version directories or files
            for item in type_dir.iterdir():
                if item.is_dir():
                    # Version directory (e.g., v1.0, v1.1)
                    version = item.name.replace('v', '')
                    self._load_template_version(template_type, version, item)
                elif item.suffix == '.html':
                    # Direct HTML file (default version)
                    version = self.config['default_version']
                    self._load_template_file(template_type, version, item)
                    
        except Exception as e:
            logger.error(f"Error loading template type {template_type}: {str(e)}")
    
    def _load_template_version(self, template_type: str, version: str, version_dir: Path):
        """Load a specific version of a template."""
        try:
            config_file = version_dir / 'config.json'
            template_file = version_dir / 'template.html'
            
            # Load configuration
            config = {}
            if config_file.exists():
                with open(config_file, 'r') as f:
                    config = json.load(f)
            
            # Load template
            if template_file.exists():
                template_version = TemplateVersion(
                    name=template_type,
                    version=version,
                    template_path=template_file,
                    config=config
                )
                
                self._template_registry[template_type][version] = template_version
                logger.debug(f"Loaded template {template_type} v{version}")
                
        except Exception as e:
            logger.error(f"Error loading template version {template_type} v{version}: {str(e)}")
    
    def _load_template_file(self, template_type: str, version: str, template_file: Path):
        """Load a template from a direct HTML file."""
        try:
            template_version = TemplateVersion(
                name=template_type,
                version=version,
                template_path=template_file,
                config={'is_active': True, 'created_date': datetime.utcnow().isoformat()}
            )
            
            if template_type not in self._template_registry:
                self._template_registry[template_type] = {}
                
            self._template_registry[template_type][version] = template_version
            logger.debug(f"Loaded template file {template_type} v{version}")
            
        except Exception as e:
            logger.error(f"Error loading template file {template_type} v{version}: {str(e)}")
    
    def get_template(self, template_name: str, version: Optional[str] = None) -> Optional[Template]:
        """
        Get a specific template by name and version.
        
        Args:
            template_name: Name of the template (monthly, quarterly, annual)
            version: Version of the template (defaults to latest active)
            
        Returns:
            Optional[Template]: Jinja2 template object or None if not found
        """
        try:
            # Use cache key
            cache_key = f"{template_name}:{version or 'latest'}"
            
            if cache_key in self._template_cache:
                return self._template_cache[cache_key]
            
            # Find template version
            template_version = self._find_template_version(template_name, version)
            if not template_version:
                logger.error(f"Template not found: {template_name} v{version}")
                return None
            
            # Load template content
            template_path = template_version.template_path.relative_to(self.templates_dir)
            template = self.jinja_env.get_template(str(template_path))
            
            # Cache the template
            self._template_cache[cache_key] = template
            
            logger.debug(f"Retrieved template {template_name} v{template_version.version}")
            return template
            
        except TemplateNotFound:
            logger.error(f"Template file not found: {template_name} v{version}")
            return None
        except Exception as e:
            logger.error(f"Error retrieving template: {str(e)}")
            return None
    
    def _find_template_version(self, template_name: str, version: Optional[str] = None) -> Optional[TemplateVersion]:
        """
        Find a specific template version.
        
        Args:
            template_name: Name of the template
            version: Version string or None for latest
            
        Returns:
            Optional[TemplateVersion]: Template version or None
        """
        if template_name not in self._template_registry:
            return None
        
        template_versions = self._template_registry[template_name]
        
        if version:
            return template_versions.get(version)
        
        # Find latest active version
        active_versions = [
            tv for tv in template_versions.values() 
            if tv.is_active
        ]
        
        if not active_versions:
            return None
        
        # Sort by version (assuming semantic versioning)
        active_versions.sort(key=lambda x: x.version, reverse=True)
        return active_versions[0]
    
    def get_available_templates(self) -> Dict[str, List[str]]:
        """
        Get all available templates and their versions.
        
        Returns:
            Dict[str, List[str]]: Template names mapped to available versions
        """
        result = {}
        
        for template_name, versions in self._template_registry.items():
            result[template_name] = list(versions.keys())
        
        return result
    
    def select_template(self, metadata: Dict) -> Optional[Template]:
        """
        Select appropriate template based on metadata.
        
        Args:
            metadata: Statement metadata containing template info
            
        Returns:
            Optional[Template]: Selected template or None
        """
        template_name = metadata.get('template_name', self.config['default_template'])
        template_version = metadata.get('template_version', self.config['default_version'])
        
        logger.info(f"Selecting template: {template_name} v{template_version}")
        
        template = self.get_template(template_name, template_version)
        
        if not template:
            # Fallback to default template
            logger.warning(f"Template {template_name} v{template_version} not found, using default")
            template = self.get_template(
                self.config['default_template'],
                self.config['default_version']
            )
        
        return template
    
    def _register_custom_filters(self):
        """Register custom Jinja2 filters for financial data formatting."""
        
        def currency_filter(value: float, currency_code: str = 'USD') -> str:
            """Format a number as currency."""
            if currency_code == 'USD':
                return f"${value:,.2f}"
            return f"{value:,.2f} {currency_code}"
        
        def date_filter(date_string: str, format_type: str = 'short') -> str:
            """Format a date string."""
            try:
                date_obj = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
                
                if format_type == 'short':
                    return date_obj.strftime('%m/%d/%Y')
                elif format_type == 'long':
                    return date_obj.strftime('%B %d, %Y')
                elif format_type == 'iso':
                    return date_obj.strftime('%Y-%m-%d')
                else:
                    return date_obj.strftime(format_type)
                    
            except ValueError:
                return date_string
        
        def percentage_filter(value: float, precision: int = 2) -> str:
            """Format a decimal as percentage."""
            return f"{value * 100:.{precision}f}%"
        
        def account_number_filter(account_number: str) -> str:
            """Format account number with masking."""
            if len(account_number) > 4:
                return f"****{account_number[-4:]}"
            return account_number
        
        # Register filters
        self.jinja_env.filters['currency'] = currency_filter
        self.jinja_env.filters['date'] = date_filter
        self.jinja_env.filters['percentage'] = percentage_filter
        self.jinja_env.filters['account_number'] = account_number_filter
    
    def render_template(self, template: Template, data: Dict) -> str:
        """
        Render a template with financial statement data.
        
        Args:
            template: Jinja2 template object
            data: Statement data to render
            
        Returns:
            str: Rendered HTML content
        """
        try:
            # Add helper functions to context
            context = {
                **data,
                'now': datetime.utcnow(),
                'format_currency': lambda x, c='USD': self.jinja_env.filters['currency'](x, c)
            }
            
            return template.render(**context)
            
        except Exception as e:
            logger.error(f"Error rendering template: {str(e)}")
            raise
    
    def clear_cache(self):
        """Clear the template cache."""
        self._template_cache.clear()
        logger.info("Template cache cleared")