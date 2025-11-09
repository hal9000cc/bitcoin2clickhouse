#!/usr/bin/env python3

import os
import sys
import argparse
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

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Database integrity check for Bitcoin2ClickHouse',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                    # Run all checks
  %(prog)s -c 1 2 3          # Run checks 1, 2, and 3
  %(prog)s --check 8         # Run only check 8 (turnover completeness)
  
Available checks:
  1 - Genesis block
  2 - Block sequence
  3 - Transaction count consistency
  4 - n_block consistency
  5 - Previous block hash consistency
  6 - Transaction hash consistency
  7 - Previous transaction hash consistency
  8 - Turnover completeness
        """
    )
    
    parser.add_argument(
        '-c', '--check',
        type=int,
        nargs='+',
        metavar='N',
        help='Check number(s) to run (1-8). If not specified, all checks will run.'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        metavar='SIZE',
        help='Batch size for checks that process blocks in batches (default: 10000). Ignored for checks that do not use batches.'
    )
    
    parser.add_argument(
        '--start-block',
        type=int,
        metavar='BLOCK',
        help='Start block number for checks that process blocks. Ignored for checks that do not use blocks.'
    )
    
    parser.add_argument(
        '--end-block',
        type=int,
        metavar='BLOCK',
        help='End block number for checks that process blocks. Ignored for checks that do not use blocks.'
    )
    
    return parser.parse_args()

def main():
    """Script for database integrity check"""
    args = parse_arguments()
    load_dotenv()
    
    log_filename = setup_logging()
    logger = logging.getLogger(__name__)
    
    clickhouse_host = os.getenv('CLICKHOUSE_HOST', 'localhost')
    clickhouse_port = int(os.getenv('CLICKHOUSE_PORT', '9000'))
    clickhouse_user = os.getenv('CLICKHOUSE_USER', 'default')
    clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', '')
    clickhouse_database = os.getenv('CLICKHOUSE_DATABASE', 'bitcoin')
    
    if args.check:
        invalid_checks = [c for c in args.check if c < 1 or c > 8]
        if invalid_checks:
            logger.error(f"Invalid check numbers: {invalid_checks}. Valid range is 1-8.")
            return 1
        check_numbers = sorted(set(args.check)) 
        logger.info(f"Starting database integrity check (checks: {', '.join(map(str, check_numbers))})")
    else:
        check_numbers = None
        logger.info("Starting database integrity check (all checks)")
    
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
        
        errors = loader.check_database_integrity(
            check_numbers=check_numbers,
            batch_size=args.batch_size,
            start_block=args.start_block,
            end_block=args.end_block
        )
        
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
