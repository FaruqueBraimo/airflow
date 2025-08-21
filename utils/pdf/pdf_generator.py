"""
PDF generation module for financial statements using ReportLab.
"""
import io
import logging
from pathlib import Path
from typing import Dict, Optional, Union
from datetime import datetime
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.colors import HexColor, black, white
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.platypus.flowables import HRFlowable
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from config.airflow_config import PDF_CONFIG, OUTPUT_DIR
from utils.templates.template_manager import TemplateManager
import weasyprint

logger = logging.getLogger(__name__)


class PDFGenerator:
    """
    Professional PDF generator for financial statements.
    """
    
    def __init__(self, output_dir: Optional[Path] = None):
        """
        Initialize PDF generator.
        
        Args:
            output_dir: Directory to save generated PDFs
        """
        self.output_dir = Path(output_dir or OUTPUT_DIR)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.config = PDF_CONFIG
        self.template_manager = TemplateManager()
        
    def generate_pdf_from_html(self, html_content: str, output_path: Path) -> bool:
        """
        Generate PDF from HTML content using WeasyPrint.
        
        Args:
            html_content: Rendered HTML content
            output_path: Path to save the PDF
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Configure WeasyPrint
            html_doc = weasyprint.HTML(string=html_content)
            
            # Generate PDF
            html_doc.write_pdf(str(output_path))
            
            logger.info(f"Successfully generated PDF: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error generating PDF from HTML: {str(e)}")
            return False
    
    def generate_statement_pdf(self, statement_data: Dict, template_name: Optional[str] = None) -> Optional[Path]:
        """
        Generate a complete financial statement PDF.
        
        Args:
            statement_data: Validated statement data
            template_name: Override template selection
            
        Returns:
            Optional[Path]: Path to generated PDF or None if failed
        """
        try:
            # Select template
            metadata = statement_data.get('metadata', {})
            if template_name:
                metadata['template_name'] = template_name
                
            template = self.template_manager.select_template(metadata)
            if not template:
                logger.error("No template found for statement generation")
                return None
            
            # Render HTML content
            html_content = self.template_manager.render_template(template, statement_data)
            
            # Generate output filename
            output_filename = self._generate_filename(statement_data)
            output_path = self.output_dir / output_filename
            
            # Generate PDF
            if self.generate_pdf_from_html(html_content, output_path):
                return output_path
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error generating statement PDF: {str(e)}")
            return None
    
    def generate_pdf_reportlab(self, statement_data: Dict) -> Optional[Path]:
        """
        Generate PDF using ReportLab (alternative method).
        
        Args:
            statement_data: Validated statement data
            
        Returns:
            Optional[Path]: Path to generated PDF or None if failed
        """
        try:
            # Generate output filename
            output_filename = self._generate_filename(statement_data)
            output_path = self.output_dir / output_filename
            
            # Create PDF document
            doc = SimpleDocTemplate(
                str(output_path),
                pagesize=letter,
                rightMargin=self.config['margins']['right'],
                leftMargin=self.config['margins']['left'],
                topMargin=self.config['margins']['top'],
                bottomMargin=self.config['margins']['bottom']
            )
            
            # Build content
            story = []
            styles = getSampleStyleSheet()
            
            # Custom styles
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=self.config['font_size']['title'],
                spaceAfter=30,
                alignment=TA_CENTER
            )
            
            heading_style = ParagraphStyle(
                'CustomHeading',
                parent=styles['Heading2'],
                fontSize=self.config['font_size']['heading'],
                spaceAfter=12,
                textColor=HexColor('#2c5f7d')
            )
            
            body_style = ParagraphStyle(
                'CustomBody',
                parent=styles['Normal'],
                fontSize=self.config['font_size']['body'],
                spaceAfter=6
            )
            
            # Header
            story.append(Paragraph("Financial Services Corp", title_style))
            story.append(Paragraph(f"{statement_data.get('statement_type', 'Monthly').title()} Statement", heading_style))
            story.append(Spacer(1, 20))
            
            # Customer info
            customer_info = statement_data.get('customer_info', {})
            story.append(Paragraph("Customer Information", heading_style))
            story.append(Paragraph(f"<b>Name:</b> {customer_info.get('name', 'N/A')}", body_style))
            story.append(Paragraph(f"<b>Customer ID:</b> {statement_data.get('customer_id', 'N/A')}", body_style))
            story.append(Paragraph(f"<b>Statement Date:</b> {statement_data.get('statement_date', 'N/A')}", body_style))
            story.append(Spacer(1, 20))
            
            # Account balances
            balances = statement_data.get('balances', {})
            if balances:
                story.append(Paragraph("Account Summary", heading_style))
                
                balance_data = [['Account Type', 'Opening Balance', 'Closing Balance', 'Net Change']]
                for account_type, balance in balances.items():
                    if isinstance(balance, dict):
                        opening = balance.get('opening_balance', 0)
                        closing = balance.get('closing_balance', 0)
                        net_change = closing - opening
                        balance_data.append([
                            account_type.title(),
                            f"${opening:,.2f}",
                            f"${closing:,.2f}",
                            f"${net_change:,.2f}"
                        ])
                
                if len(balance_data) > 1:
                    balance_table = Table(balance_data, colWidths=[2*inch, 1.5*inch, 1.5*inch, 1.5*inch])
                    balance_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, 0), 10),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black)
                    ]))
                    story.append(balance_table)
                    story.append(Spacer(1, 20))
            
            # Transactions
            transactions = statement_data.get('transactions', [])
            if transactions:
                story.append(Paragraph("Recent Transactions", heading_style))
                
                trans_data = [['Date', 'Description', 'Amount']]
                for i, transaction in enumerate(transactions[:10]):  # Show first 10 transactions
                    trans_data.append([
                        transaction.get('date', 'N/A')[:10],  # Date only
                        transaction.get('description', 'N/A')[:40],  # Truncate long descriptions
                        f"${transaction.get('amount', 0):,.2f}"
                    ])
                
                if len(trans_data) > 1:
                    trans_table = Table(trans_data, colWidths=[1.5*inch, 3.5*inch, 1.5*inch])
                    trans_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                        ('ALIGN', (2, 0), (2, -1), 'RIGHT'),  # Right-align amounts
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, 0), 10),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black)
                    ]))
                    story.append(trans_table)
                
                if len(transactions) > 10:
                    story.append(Spacer(1, 12))
                    story.append(Paragraph(f"... and {len(transactions) - 10} more transactions", body_style))
            
            # Summary totals
            totals = statement_data.get('totals', {})
            if totals:
                story.append(Spacer(1, 20))
                story.append(Paragraph("Summary", heading_style))
                
                for key, value in totals.items():
                    if isinstance(value, (int, float)):
                        story.append(Paragraph(f"<b>{key.replace('_', ' ').title()}:</b> ${value:,.2f}", body_style))
                    else:
                        story.append(Paragraph(f"<b>{key.replace('_', ' ').title()}:</b> {value}", body_style))
            
            # Footer
            story.append(Spacer(1, 40))
            story.append(HRFlowable(width="100%", thickness=1, color=colors.grey))
            story.append(Spacer(1, 12))
            footer_text = f"Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}"
            story.append(Paragraph(footer_text, ParagraphStyle('Footer', parent=styles['Normal'], fontSize=8, alignment=TA_CENTER)))
            
            # Build PDF
            doc.build(story)
            
            logger.info(f"Successfully generated ReportLab PDF: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error generating ReportLab PDF: {str(e)}")
            return None
    
    def _generate_filename(self, statement_data: Dict) -> str:
        """
        Generate a filename for the PDF based on statement data.
        
        Args:
            statement_data: Statement data
            
        Returns:
            str: Generated filename
        """
        try:
            statement_id = statement_data.get('statement_id', 'unknown')
            customer_id = statement_data.get('customer_id', 'unknown')
            statement_type = statement_data.get('statement_type', 'statement')
            
            # Clean IDs for filename
            statement_id = ''.join(c for c in statement_id if c.isalnum() or c in '-_')[:20]
            customer_id = ''.join(c for c in customer_id if c.isalnum() or c in '-_')[:20]
            
            # Add timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            return f"{statement_type}_{customer_id}_{statement_id}_{timestamp}.pdf"
            
        except Exception as e:
            logger.warning(f"Error generating filename: {str(e)}")
            return f"statement_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    
    def validate_pdf_output(self, pdf_path: Path) -> bool:
        """
        Validate that the generated PDF is valid.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            bool: True if valid, False otherwise
        """
        try:
            if not pdf_path.exists():
                return False
                
            # Basic size check
            if pdf_path.stat().st_size < 1000:  # Less than 1KB is likely invalid
                logger.warning(f"PDF file too small: {pdf_path.stat().st_size} bytes")
                return False
            
            # Check if file starts with PDF header
            with open(pdf_path, 'rb') as f:
                header = f.read(4)
                if header != b'%PDF':
                    logger.error("Invalid PDF header")
                    return False
            
            logger.debug(f"PDF validation passed: {pdf_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error validating PDF: {str(e)}")
            return False
    
    def batch_generate_pdfs(self, statements: list) -> Dict[str, Optional[Path]]:
        """
        Generate multiple PDFs in batch.
        
        Args:
            statements: List of statement data dictionaries
            
        Returns:
            Dict[str, Optional[Path]]: Statement IDs mapped to PDF paths
        """
        results = {}
        
        for statement_data in statements:
            try:
                statement_id = statement_data.get('statement_id', f'unknown_{len(results)}')
                pdf_path = self.generate_statement_pdf(statement_data)
                results[statement_id] = pdf_path
                
                if pdf_path:
                    logger.info(f"Generated PDF for statement {statement_id}")
                else:
                    logger.error(f"Failed to generate PDF for statement {statement_id}")
                    
            except Exception as e:
                logger.error(f"Error in batch processing: {str(e)}")
                results[statement_id] = None
        
        logger.info(f"Batch processing complete: {len([p for p in results.values() if p])} successful, "
                   f"{len([p for p in results.values() if not p])} failed")
        
        return results


class PDFMetadataInjector:
    """
    Utility class to inject metadata into generated PDFs.
    """
    
    @staticmethod
    def add_metadata(pdf_path: Path, metadata: Dict) -> bool:
        """
        Add metadata to an existing PDF.
        
        Args:
            pdf_path: Path to the PDF file
            metadata: Metadata to add
            
        Returns:
            bool: True if successful
        """
        try:
            # This would require PyPDF2 or similar library
            # Implementation depends on specific requirements
            logger.info(f"Metadata injection requested for {pdf_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error injecting metadata: {str(e)}")
            return False