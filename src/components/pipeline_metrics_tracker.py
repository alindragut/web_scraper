import re
import logging
import time
from collections import Counter

# Get a logger instance for this specific module.
log = logging.getLogger(__name__)

class PipelineMetricsTracker:
    """A stateful class to track pipeline metrics over time."""
    def __init__(self):
        # For Coverage
        self.produced_urls = set()
        self.fetched_urls = set()
        
        # For Fill Rates
        self.fields_to_check = ['phone_numbers', 'social_media_links', 'addresses']
        self.field_counts = Counter()

    def process_log_event(self, log_entry: dict):
        """Processes a single log entry."""
        service = log_entry.get('service')
        message = log_entry.get('message', '')

        if service == 'URLProducer' and 'Produced message for URL:' in message:
            match = re.search(r"Produced message for URL: (.*)", message)
            if match:
                produced_url = match.group(1).strip()
                
                if produced_url in self.produced_urls:
                    log.warning(f"Duplicate produced URL found: {produced_url}")
                else:
                    self.produced_urls.add(produced_url)
        elif service == 'FetcherService' and 'Successfully fetched and produced:' in message:
            match = re.search(r"Successfully fetched and produced: (.*)", message)
            if match:
                fetched_url = match.group(1).strip()
                
                if fetched_url in self.fetched_urls:
                    log.warning(f"Duplicate fetched URL found: {fetched_url}")
                else:
                    self.fetched_urls.add(fetched_url)

    def process_extracted_data(self, record: dict):
        """Processes a single company record."""
        for field in self.fields_to_check:
            value = record.get(field)
            if value:
                self.field_counts[field] += 1
    
    def generate_report(self) -> dict:
        """Calculates and returns a report with Coverage and Fill Rates metrics."""
        # Coverage
        total_produced = len(self.produced_urls)
        total_fetched = len(self.fetched_urls)
        coverage_percent = (total_fetched / total_produced * 100) if total_produced > 0 else 0
        
        # Fill Rates
        fill_rates = {}
        if total_produced > 0:
            for field in self.fields_to_check:
                count = self.field_counts.get(field, 0)
                fill_rate_percent = (count / total_produced * 100)
                fill_rates[field] = {
                    "count": count,
                    "fill_rate_percent": round(fill_rate_percent, 2)
                }

        return {
            "report_type": "pipeline_metrics",
            "timestamp": time.time(),
            "coverage": {
                "urls_produced": total_produced,
                "urls_fetched": total_fetched,
                "coverage_percent": round(coverage_percent, 2)
            },
            "fill_rates": {
                "total_records_processed": total_produced,
                "fields": fill_rates
            }
        }