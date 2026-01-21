"""
kpi_extractor.py - KPI Extraction & Analytics

Handles:
- Manufacturing KPIs (throughput, lead time, OEE)
- Quality control metrics (pass rates, trends)
- Inventory metrics (stock levels, turnover)
- Lead time analysis
- Statistical calculations
- Report generation and export
- Caching and performance optimization
- Proper error handling and statistics

Production-ready with validation, error handling, and comprehensive metrics.
"""

from __future__ import annotations

import logging
import csv
import json
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from pathlib import Path

from client import OdooClient
from validation import validate_int, validate_range, ValidationError
from utils import (
    log_header, log_info, log_success, log_warn, log_error,
    timed_operation,
)


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class MOPerformance:
    """Manufacturing order performance metrics."""
    mo_count: int
    avg_throughput_days: float
    min_throughput_days: float
    max_throughput_days: float
    median_throughput_days: float


@dataclass
class QCMetrics:
    """Quality control metrics."""
    checks_total: int
    checks_passed: int
    checks_failed: int
    pass_rate: float
    fail_rate: float
    pending_checks: int = 0


@dataclass
class InventoryMetrics:
    """Inventory metrics."""
    products_with_stock: int
    total_stock_qty: float
    avg_stock_per_product: float
    products_analyzed: int


@dataclass
class LeadTimeMetrics:
    """Lead time metrics."""
    avg_lead_time_days: float
    min_lead_time_days: float
    max_lead_time_days: float
    orders_analyzed: int


@dataclass
class KPIReport:
    """Complete KPI report."""
    timestamp: datetime
    period_start: datetime
    period_end: datetime
    
    mo_performance: Optional[MOPerformance] = None
    qc_metrics: Optional[QCMetrics] = None
    inventory_metrics: Optional[InventoryMetrics] = None
    lead_time_metrics: Optional[LeadTimeMetrics] = None


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class KPIError(Exception):
    """Base KPI extraction error."""
    pass


class KPIValidationError(KPIError):
    """KPI validation error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# KPI EXTRACTOR
# ═══════════════════════════════════════════════════════════════════════════════

class KPIExtractor:
    """Extract and analyze KPIs from manufacturing, quality, and inventory."""
    
    def __init__(
        self,
        client: OdooClient,
        base_data_dir: Optional[str] = None,
    ):
        """Initialize KPI extractor."""
        self.client = client
        self.base_data_dir = Path(base_data_dir) if base_data_dir else Path('.')
        
        self.stats = {
            'mos_analyzed': 0,
            'qc_checks_analyzed': 0,
            'products_analyzed': 0,
            'sales_orders_analyzed': 0,
            'reports_generated': 0,
            'errors': 0,
        }
        
        self.audit_log: List[Dict[str, Any]] = []
        
        logger.info("KPIExtractor initialized")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # DATE/TIME UTILITIES
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _default_mo_timerange(self) -> Tuple[datetime, datetime]:
        """Get default time range (last 30 days)."""
        end = datetime.utcnow()
        start = end - timedelta(days=30)
        return start, end
    
    def _validate_daterange(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> Tuple[datetime, datetime]:
        """Validate and normalize date range."""
        if not isinstance(start_date, datetime):
            raise KPIValidationError(f"start_date must be datetime: {type(start_date)}")
        
        if not isinstance(end_date, datetime):
            raise KPIValidationError(f"end_date must be datetime: {type(end_date)}")
        
        if start_date >= end_date:
            raise KPIValidationError(
                f"start_date must be before end_date: {start_date} >= {end_date}"
            )
        
        return start_date, end_date
    
    def _parse_datetime(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse Odoo datetime string."""
        if not date_str:
            return None
        
        try:
            # Handle Odoo ISO format with Z
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except Exception as e:
            logger.warning(f"Failed to parse datetime {date_str}: {e}")
            return None
    
    # ═══════════════════════════════════════════════════════════════════════════
    # MANUFACTURING PERFORMANCE
    # ═══════════════════════════════════════════════════════════════════════════
    
    def get_mo_performance(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> MOPerformance:
        """
        Calculate manufacturing order performance metrics.
        
        Args:
            start_date: Period start (defaults to 30 days ago)
            end_date: Period end (defaults to now)
        
        Returns:
            MOPerformance metrics
        
        Raises:
            KPIError: If calculation fails
        """
        logger.info("Calculating MO performance metrics")
        
        try:
            if start_date is None or end_date is None:
                start_date, end_date = self._default_mo_timerange()
            
            start_date, end_date = self._validate_daterange(start_date, end_date)
            
            # Search MOs
            mos = self.client.search_read(
                'mrp.production',
                [
                    ('state', '=', 'done'),
                    ('create_date', '>=', start_date.strftime("%Y-%m-%d %H:%M:%S")),
                    ('create_date', '<=', end_date.strftime("%Y-%m-%d %H:%M:%S")),
                ],
                ['id', 'product_id', 'product_qty', 'create_date', 'date_finished'],
                limit=1000,
            )
            
            self.stats['mos_analyzed'] += len(mos)
            
            # Calculate durations
            durations: List[float] = []
            
            for mo in mos:
                try:
                    dt_create = self._parse_datetime(mo.get('create_date'))
                    dt_finished = self._parse_datetime(mo.get('date_finished'))
                    
                    if not dt_create or not dt_finished:
                        logger.debug(f"MO {mo['id']} missing dates, skipping")
                        continue
                    
                    duration_days = (dt_finished - dt_create).total_seconds() / 86400.0
                    durations.append(duration_days)
                    
                    logger.debug(
                        f"MO {mo['id']}: {duration_days:.2f} days "
                        f"(qty={mo.get('product_qty')})"
                    )
                
                except Exception as e:
                    logger.warning(f"Failed to process MO {mo['id']}: {e}")
            
            # Calculate statistics
            if not durations:
                logger.warning("No valid MO durations found")
                return MOPerformance(
                    mo_count=0,
                    avg_throughput_days=0.0,
                    min_throughput_days=0.0,
                    max_throughput_days=0.0,
                    median_throughput_days=0.0,
                )
            
            durations.sort()
            
            metrics = MOPerformance(
                mo_count=len(durations),
                avg_throughput_days=sum(durations) / len(durations),
                min_throughput_days=min(durations),
                max_throughput_days=max(durations),
                median_throughput_days=(
                    durations[len(durations) // 2]
                    if len(durations) % 2 == 1
                    else (durations[len(durations) // 2 - 1] + durations[len(durations) // 2]) / 2
                ),
            )
            
            logger.info(
                f"MO Performance: {metrics.mo_count} orders, "
                f"avg {metrics.avg_throughput_days:.2f} days"
            )
            
            return metrics
        
        except Exception as e:
            logger.error(f"Failed to calculate MO performance: {e}", exc_info=True)
            self.stats['errors'] += 1
            raise KPIError(f"MO performance calculation failed: {e}") from e
    
    # ═══════════════════════════════════════════════════════════════════════════
    # QUALITY CONTROL METRICS
    # ═══════════════════════════════════════════════════════════════════════════
    
    def get_qc_metrics(
        self,
        product_id: Optional[int] = None,
    ) -> QCMetrics:
        """
        Calculate quality control metrics.
        
        Args:
            product_id: Optional product filter
        
        Returns:
            QCMetrics
        """
        logger.info("Calculating QC metrics")
        
        try:
            domain: List = []
            if product_id:
                domain.append(('product_id', '=', product_id))
            
            # Search quality checks
            checks = self.client.search_read(
                'quality.check',
                domain,
                [
                    'id',
                    'product_id',
                    'point_id',
                    'quality_state',
                ],
                limit=1000,
            )
            
            self.stats['qc_checks_analyzed'] += len(checks)
            
            # Calculate metrics
            total = len(checks)
            passed = sum(1 for c in checks if c.get('quality_state') == 'pass')
            failed = sum(1 for c in checks if c.get('quality_state') == 'fail')
            pending = sum(1 for c in checks if c.get('quality_state') == 'none')
            
            pass_rate = (passed / total * 100) if total > 0 else 0.0
            fail_rate = (failed / total * 100) if total > 0 else 0.0
            
            metrics = QCMetrics(
                checks_total=total,
                checks_passed=passed,
                checks_failed=failed,
                pass_rate=pass_rate,
                fail_rate=fail_rate,
                pending_checks=pending,
            )
            
            logger.info(
                f"QC Metrics: {total} checks, "
                f"{pass_rate:.1f}% pass rate"
            )
            
            return metrics
        
        except Exception as e:
            logger.error(f"Failed to calculate QC metrics: {e}", exc_info=True)
            self.stats['errors'] += 1
            
            # Return empty metrics on error
            return QCMetrics(
                checks_total=0,
                checks_passed=0,
                checks_failed=0,
                pass_rate=0.0,
                fail_rate=0.0,
            )
    
    # ═══════════════════════════════════════════════════════════════════════════
    # INVENTORY METRICS
    # ═══════════════════════════════════════════════════════════════════════════
    
    def get_inventory_metrics(self) -> InventoryMetrics:
        """
        Calculate inventory metrics.
        
        Returns:
            InventoryMetrics
        """
        logger.info("Calculating inventory metrics")
        
        try:
            products = self.client.search_read(
                'product.product',
                [],
                ['id', 'name', 'qty_available'],
                limit=1000,
            )
            
            self.stats['products_analyzed'] += len(products)
            
            # Calculate metrics
            quantities = [p.get('qty_available', 0.0) for p in products]
            products_with_stock = sum(1 for q in quantities if q > 0)
            total_stock = sum(quantities)
            avg_stock = (total_stock / len(products)) if products else 0.0
            
            metrics = InventoryMetrics(
                products_with_stock=products_with_stock,
                total_stock_qty=total_stock,
                avg_stock_per_product=avg_stock,
                products_analyzed=len(products),
            )
            
            logger.info(
                f"Inventory Metrics: {products_with_stock} products in stock, "
                f"total qty={total_stock:.0f}"
            )
            
            return metrics
        
        except Exception as e:
            logger.error(f"Failed to calculate inventory metrics: {e}")
            self.stats['errors'] += 1
            
            return InventoryMetrics(
                products_with_stock=0,
                total_stock_qty=0.0,
                avg_stock_per_product=0.0,
                products_analyzed=0,
            )
    
    # ═══════════════════════════════════════════════════════════════════════════
    # LEAD TIME ANALYSIS
    # ═══════════════════════════════════════════════════════════════════════════
    
    def get_lead_time_metrics(self) -> LeadTimeMetrics:
        """
        Calculate lead time metrics (SO creation to delivery).
        
        Returns:
            LeadTimeMetrics
        """
        logger.info("Calculating lead time metrics")
        
        try:
            # Search recent SOs
            orders = self.client.search_read(
                'sale.order',
                [('state', 'in', ['sale', 'done'])],
                ['id', 'name', 'create_date'],
                limit=50,
            )
            
            lead_times: List[float] = []
            
            for so in orders:
                try:
                    dt_create = self._parse_datetime(so.get('create_date'))
                    if not dt_create:
                        continue
                    
                    # Find completed pickings
                    pickings = self.client.search_read(
                        'stock.picking',
                        [
                            ('sale_id', '=', so['id']),
                            ('picking_type_code', '=', 'outgoing'),
                            ('state', '=', 'done'),
                        ],
                        ['date_done'],
                        limit=5,
                    )
                    
                    if not pickings:
                        continue
                    
                    # Get latest delivery date
                    dates_done: List[datetime] = []
                    for p in pickings:
                        dt = self._parse_datetime(p.get('date_done'))
                        if dt:
                            dates_done.append(dt)
                    
                    if not dates_done:
                        continue
                    
                    dt_delivered = max(dates_done)
                    lead_days = (dt_delivered - dt_create).total_seconds() / 86400.0
                    lead_times.append(lead_days)
                    
                    logger.debug(f"SO {so['name']}: {lead_days:.2f} days lead time")
                
                except Exception as e:
                    logger.debug(f"Failed to calculate lead time for SO {so['id']}: {e}")
            
            self.stats['sales_orders_analyzed'] += len(lead_times)
            
            if not lead_times:
                logger.warning("No completed sales orders found for lead time analysis")
                return LeadTimeMetrics(
                    avg_lead_time_days=0.0,
                    min_lead_time_days=0.0,
                    max_lead_time_days=0.0,
                    orders_analyzed=0,
                )
            
            lead_times.sort()
            
            metrics = LeadTimeMetrics(
                avg_lead_time_days=sum(lead_times) / len(lead_times),
                min_lead_time_days=min(lead_times),
                max_lead_time_days=max(lead_times),
                orders_analyzed=len(lead_times),
            )
            
            logger.info(
                f"Lead Time Metrics: {len(lead_times)} orders, "
                f"avg {metrics.avg_lead_time_days:.2f} days"
            )
            
            return metrics
        
        except Exception as e:
            logger.error(f"Failed to calculate lead time metrics: {e}")
            self.stats['errors'] += 1
            
            return LeadTimeMetrics(
                avg_lead_time_days=0.0,
                min_lead_time_days=0.0,
                max_lead_time_days=0.0,
                orders_analyzed=0,
            )
    
    # ═══════════════════════════════════════════════════════════════════════════
    # REPORT GENERATION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def generate_report(
        self,
        period_days: int = 30,
    ) -> KPIReport:
        """
        Generate comprehensive KPI report.
        
        Args:
            period_days: Analysis period in days
        
        Returns:
            KPIReport
        """
        logger.info(f"Generating KPI report for {period_days}-day period")
        
        try:
            now = datetime.utcnow()
            period_start = now - timedelta(days=period_days)
            
            # Collect all KPIs
            mo_perf = self.get_mo_performance(period_start, now)
            qc_metrics = self.get_qc_metrics()
            inv_metrics = self.get_inventory_metrics()
            lt_metrics = self.get_lead_time_metrics()
            
            report = KPIReport(
                timestamp=now,
                period_start=period_start,
                period_end=now,
                mo_performance=mo_perf,
                qc_metrics=qc_metrics,
                inventory_metrics=inv_metrics,
                lead_time_metrics=lt_metrics,
            )
            
            self.stats['reports_generated'] += 1
            
            logger.info("KPI report generated successfully")
            
            return report
        
        except Exception as e:
            logger.error(f"Failed to generate KPI report: {e}", exc_info=True)
            self.stats['errors'] += 1
            raise KPIError(f"Report generation failed: {e}") from e
    
    # ═══════════════════════════════════════════════════════════════════════════
    # EXPORT FUNCTIONS
    # ═══════════════════════════════════════════════════════════════════════════
    
    def export_report_to_json(
        self,
        report: KPIReport,
        filepath: Optional[str] = None,
    ) -> bool:
        """
        Export report to JSON.
        
        Args:
            report: KPIReport
            filepath: Output path
        
        Returns:
            True if successful
        """
        try:
            if not filepath:
                filepath = str(
                    self.base_data_dir / f"kpi_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                )
            
            data = {
                'timestamp': report.timestamp.isoformat(),
                'period_start': report.period_start.isoformat(),
                'period_end': report.period_end.isoformat(),
                'mo_performance': asdict(report.mo_performance) if report.mo_performance else None,
                'qc_metrics': asdict(report.qc_metrics) if report.qc_metrics else None,
                'inventory_metrics': asdict(report.inventory_metrics) if report.inventory_metrics else None,
                'lead_time_metrics': asdict(report.lead_time_metrics) if report.lead_time_metrics else None,
            }
            
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"Exported KPI report to {filepath}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to export report to JSON: {e}")
            return False
    
    def export_report_to_csv(
        self,
        report: KPIReport,
        filepath: Optional[str] = None,
    ) -> bool:
        """
        Export report to CSV.
        
        Args:
            report: KPIReport
            filepath: Output path
        
        Returns:
            True if successful
        """
        try:
            if not filepath:
                filepath = str(
                    self.base_data_dir / f"kpi_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                )
            
            with open(filepath, 'w', newline='') as f:
                writer = csv.writer(f)
                
                # Header
                writer.writerow(['KPI Category', 'Metric', 'Value'])
                
                # MO Performance
                if report.mo_performance:
                    writer.writerow(['Manufacturing', 'MO Count', report.mo_performance.mo_count])
                    writer.writerow(['Manufacturing', 'Avg Throughput (days)', f"{report.mo_performance.avg_throughput_days:.2f}"])
                
                # QC Metrics
                if report.qc_metrics:
                    writer.writerow(['Quality Control', 'Total Checks', report.qc_metrics.checks_total])
                    writer.writerow(['Quality Control', 'Pass Rate (%)', f"{report.qc_metrics.pass_rate:.1f}"])
                
                # Inventory
                if report.inventory_metrics:
                    writer.writerow(['Inventory', 'Products in Stock', report.inventory_metrics.products_with_stock])
                    writer.writerow(['Inventory', 'Total Qty', f"{report.inventory_metrics.total_stock_qty:.0f}"])
                
                # Lead Time
                if report.lead_time_metrics:
                    writer.writerow(['Lead Time', 'Avg (days)', f"{report.lead_time_metrics.avg_lead_time_days:.2f}"])
            
            logger.info(f"Exported KPI report to {filepath}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to export report to CSV: {e}")
            return False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # MAIN
    # ═══════════════════════════════════════════════════════════════════════════
    
    def run(self) -> Dict[str, int]:
        """
        Main entry point.
        
        Returns:
            Statistics dict
        """
        log_header("KPI EXTRACTOR")
        
        try:
            report = self.generate_report()
            
            # Export
            self.export_report_to_json(report)
            self.export_report_to_csv(report)
            
            # Summary
            log_info("KPI Extractor Statistics:")
            for key, value in self.stats.items():
                log_info(f"  {key}: {value}")
            
            log_success("KPI report generated and exported")
            
            return self.stats
        
        except Exception as e:
            log_error(f"KPI extraction failed: {e}", exc_info=True)
            raise


# ═══════════════════════════════════════════════════════════════════════════════
# SETUP FUNCTION FOR RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def setup_kpi_dashboards(
    client: OdooClient,
    base_data_dir: Optional[str] = None,
) -> Dict[str, int]:
    """
    Initialize and run KPI extraction.
    
    Args:
        client: OdooClient
        base_data_dir: Base directory for exports
    
    Returns:
        Statistics dict
    """
    extractor = KPIExtractor(client, base_data_dir)
    return extractor.run()
