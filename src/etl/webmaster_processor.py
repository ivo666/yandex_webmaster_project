"""ETL processor for Webmaster data (rdl -> ppl)."""
import logging
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime

from models.database import get_db
from models.database import WebmasterData  # rdl слой
from models.ppl.models import WebmasterAggregated  # ppl слой

logger = logging.getLogger(__name__)


class WebmasterETLProcessor:
    """ETL processor for Webmaster data transformation."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def get_last_processed_id(self) -> int:
        """Get last processed ID from ppl layer."""
        try:
            with get_db() as db:
                last_id = db.query(WebmasterAggregated.id).order_by(WebmasterAggregated.id.desc()).first()
                return last_id[0] if last_id else 0
        except Exception as e:
            self.logger.error(f"Error getting last ID: {e}")
            return 0
    
    def get_new_rdl_data(self, last_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Get new data from rdl layer."""
        try:
            with get_db() as db:
                query = db.query(WebmasterData)
                
                if last_date:
                    query = query.filter(WebmasterData.date > last_date)
                
                # Get data that doesn't exist in ppl layer
                results = []
                for row in query.all():
                    exists = db.query(WebmasterAggregated).filter(
                        WebmasterAggregated.date == row.date,
                        WebmasterAggregated.query == row.query,
                        WebmasterAggregated.page_path == row.page_path,
                        WebmasterAggregated.device == row.device
                    ).first()
                    
                    if not exists:
                        results.append({
                            'date': row.date,
                            'page_path': row.page_path,
                            'query': row.query,
                            'demand': row.demand,
                            'impressions': row.impressions,
                            'clicks': row.clicks,
                            'position': row.position,
                            'device': row.device
                        })
                
                self.logger.info(f"Found {len(results)} new rows in rdl layer")
                return results
                
        except Exception as e:
            self.logger.error(f"Error getting rdl data: {e}")
            return []
    
    def apply_business_logic(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply business logic to raw data."""
        processed_data = []
        
        for row in data:
            # Ensure numeric types
            demand = int(row.get('demand', 0))
            impressions = int(row.get('impressions', 0))
            clicks = int(row.get('clicks', 0))
            position = float(row.get('position', 0.0))
            
            # Apply business rules
            # 1. demand should be >= impressions
            if impressions > demand:
                demand = impressions
            
            # 2. clicks should be <= impressions
            if clicks > impressions:
                clicks = impressions
            
            processed_row = {
                'date': row['date'],
                'page_path': row['page_path'],
                'query': row['query'],
                'device': row['device'],
                'demand': demand,
                'impressions': impressions,
                'clicks': clicks,
                'position': position
            }
            
            processed_data.append(processed_row)
        
        self.logger.info(f"Applied business logic to {len(processed_data)} rows")
        return processed_data
    
    def save_to_ppl(self, data: List[Dict[str, Any]]) -> int:
        """Save processed data to ppl layer."""
        if not data:
            return 0
        
        try:
            with get_db() as db:
                # Get next ID
                last_id = self.get_last_processed_id()
                next_id = last_id + 1
                
                saved_count = 0
                for i, row in enumerate(data, 1):
                    ppl_row = WebmasterAggregated(
                        id=next_id + i - 1,
                        date=row['date'],
                        query=row['query'],
                        page_path=row['page_path'],
                        device=row['device'],
                        demand=row['demand'],
                        impressions=row['impressions'],
                        clicks=row['clicks'],
                        position=row['position']
                    )
                    
                    db.add(ppl_row)
                    saved_count += 1
                
                self.logger.info(f"Saved {saved_count} rows to ppl layer")
                return saved_count
                
        except Exception as e:
            self.logger.error(f"Error saving to ppl: {e}")
            return 0
    
    def run_etl(self) -> int:
        """Run complete ETL process."""
        self.logger.info("Starting Webmaster ETL process...")
        
        try:
            # 1. Get last processed date
            last_date = None
            last_id = self.get_last_processed_id()
            if last_id > 0:
                with get_db() as db:
                    last_row = db.query(WebmasterAggregated).filter_by(id=last_id).first()
                    if last_row:
                        last_date = last_row.date
            
            # 2. Get new data from rdl
            new_data = self.get_new_rdl_data(last_date)
            
            if not new_data:
                self.logger.info("No new data to process")
                return 0
            
            # 3. Apply business logic
            processed_data = self.apply_business_logic(new_data)
            
            # 4. Save to ppl
            saved_count = self.save_to_ppl(processed_data)
            
            self.logger.info(f"ETL completed: {saved_count} rows processed")
            return saved_count
            
        except Exception as e:
            self.logger.error(f"ETL failed: {e}")
            return 0
