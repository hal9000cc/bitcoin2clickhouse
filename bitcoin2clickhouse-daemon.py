#!/usr/bin/env python3

import os
import sys
import time
import signal
import logging
from dotenv import load_dotenv
from src.bitcoin2clickhouse import BitcoinClickHouseLoader

class Bitcoin2ClickHouseDaemon:
    def __init__(self):
        load_dotenv()
        
        self._setup_logging()
        
        self.clickhouse_host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        self.clickhouse_port = int(os.getenv('CLICKHOUSE_PORT', '9000'))
        self.clickhouse_user = os.getenv('CLICKHOUSE_USER', 'default')
        self.clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', '')
        self.clickhouse_database = os.getenv('CLICKHOUSE_DATABASE', 'bitcoin')
        
        self.bitcoin_blocks_dir = os.getenv('BITCOIN_BLOCKS_DIR', '/home/user/.bitcoin/blocks')
        self.xor_dat_path = os.getenv('XOR_DAT_PATH', None)
        
        self.delay_minutes = int(os.getenv('DELAY_MINUTES', '10'))
        
        self.loader = None
        self.running = True
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _setup_logging(self):
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            
            file_handler = logging.FileHandler('bitcoin2clickhouse_daemon.log')
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)
            
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            
            self.logger.setLevel(logging.INFO)
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
    
    def _signal_handler(self, signum, frame):
        self.logger.info(f"Received signal {signum}. Shutting down gracefully...")
        self.running = False
    
    def _validate_environment(self):
        if not os.path.exists(self.bitcoin_blocks_dir):
            self.logger.error(f"Bitcoin blocks directory not found: {self.bitcoin_blocks_dir}")
            return False
        
        if self.xor_dat_path and not os.path.exists(self.xor_dat_path):
            self.logger.error(f"XOR dat file not found: {self.xor_dat_path}")
            return False
        
        return True
    
    def _initialize_loader(self):
        try:
            self.logger.info("Initializing Bitcoin2ClickHouse loader...")
            self.loader = BitcoinClickHouseLoader(
                clickhouse_host=self.clickhouse_host,
                clickhouse_port=self.clickhouse_port,
                clickhouse_user=self.clickhouse_user,
                clickhouse_password=self.clickhouse_password,
                database=self.clickhouse_database
            )
            self.logger.info("Loader initialized successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error initializing loader: {e}")
            return False
    
    def _load_new_blocks(self):
        self.logger.info(f"Loading new blocks from {self.bitcoin_blocks_dir}...")
        self.loader.load_new(
            blockchain_path=self.bitcoin_blocks_dir,
            xor_dat_path=self.xor_dat_path,
            batch_size=500000
        )
        self.logger.info("New blocks loaded successfully")
        return True
    
    def run(self):
        self.logger.info("Bitcoin2ClickHouse Daemon starting...")
        self.logger.info(f"ClickHouse: {self.clickhouse_host}:{self.clickhouse_port}")
        self.logger.info(f"Database: {self.clickhouse_database}")
        self.logger.info(f"Bitcoin blocks: {self.bitcoin_blocks_dir}")
        if self.xor_dat_path:
            self.logger.info(f"XOR dat file: {self.xor_dat_path}")
        self.logger.info(f"Delay: {self.delay_minutes} minutes")
        self.logger.info("-" * 50)
        
        if not self._validate_environment():
            return 1
        
        if not self._initialize_loader():
            return 1
        
        self.logger.info(f"Starting daemon with {self.delay_minutes} minute intervals...")
        self.logger.info("Press Ctrl+C to stop")
        
        while self.running:
            try:
                success = self._load_new_blocks()
                if not success:
                    self.logger.warning("Failed to load new blocks, will retry on next cycle")
                
                if self.running:
                    self.logger.info(f"Waiting {self.delay_minutes} minutes before next check...")
                    for _ in range(self.delay_minutes * 60):
                        if not self.running:
                            break
                        time.sleep(1)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                raise e
        
        self.logger.info("Daemon stopped")
        return 0

def main():
    daemon = Bitcoin2ClickHouseDaemon()
    return daemon.run()

if __name__ == "__main__":
    sys.exit(main())
