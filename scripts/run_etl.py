#!/usr/bin/env python3
"""Run Webmaster ETL process."""
import sys
import os
import logging

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Main function."""
    from etl.webmaster_processor import WebmasterETLProcessor
    
    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("WEBMASTER ETL PROCESSOR")
    logger.info("=" * 60)
    
    processor = WebmasterETLProcessor()
    result = processor.run_etl()
    
    if result > 0:
        logger.info(f"✅ Successfully processed {result} rows")
    else:
        logger.info("✅ No new data to process")
    
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
