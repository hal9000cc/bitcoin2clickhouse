#!/usr/bin/env python3

import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv
from bitcoin2clickhouse import BitcoinClickHouseLoader

def setup_logging():
    """Setup logging to file and console"""
    os.makedirs('logs', exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join('logs', f'database_check_{timestamp}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return log_filename

def main():
    """Script for database integrity check"""
    load_dotenv()
    
    log_filename = setup_logging()
    logger = logging.getLogger(__name__)
    
    clickhouse_host = os.getenv('CLICKHOUSE_HOST', 'localhost')
    clickhouse_port = int(os.getenv('CLICKHOUSE_PORT', '9000'))
    clickhouse_user = os.getenv('CLICKHOUSE_USER', 'default')
    clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', '')
    clickhouse_database = os.getenv('CLICKHOUSE_DATABASE', 'bitcoin')
    
    logger.info(f"Starting database integrity check")
    logger.info(f"Log file: {log_filename}")
    logger.info(f"Connecting to ClickHouse: {clickhouse_host}:{clickhouse_port}")
    logger.info(f"Database: {clickhouse_database}")
    logger.info("-" * 50)
    
    try:
        loader = BitcoinClickHouseLoader(
            clickhouse_host=clickhouse_host,
            clickhouse_port=clickhouse_port,
            clickhouse_user=clickhouse_user,
            clickhouse_password=clickhouse_password,
            database=clickhouse_database
        )
        
        errors = loader.check_database_integrity()
        
        logger.info("\n" + "=" * 50)
        logger.info("CHECK RESULTS")
        logger.info("=" * 50)
        
        if errors:
            logger.error(f"Found {len(errors)} errors:")
            for i, error in enumerate(errors, 1):
                logger.error(f"{i}. {error}")
            logger.info(f"Check completed with errors. Log saved to: {log_filename}")
            return 1
        else:
            logger.info("✓ All checks passed successfully. No errors found.")
            logger.info(f"Check completed successfully. Log saved to: {log_filename}")
            return 0
            
    except Exception as e:
        logger.error(f"Error during check execution: {e}")
        logger.error(f"Check failed. Log saved to: {log_filename}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
