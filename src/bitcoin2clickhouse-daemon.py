#!/usr/bin/env python3

import os
import sys
import time
import signal
import logging
import glob
from dotenv import load_dotenv
import traceback
# from blockchain_parser.blockchain import get_blocks
# from blockchain_parser.block import Block
from src.bitcoin2clickhouse import BitcoinClickHouseLoader

def format_error_with_location(error, context=""):
    """Format error with file and line number information"""
    
    tb = traceback.extract_tb(error.__traceback__)
    if tb:
        frame = tb[-1]
        filename = frame.filename.split('/')[-1]
        line_number = frame.lineno
        function_name = frame.name
        return f"{context}{error} (File: {filename}, Line: {line_number}, Function: {function_name})"
    else:
        return f"{context}{error}"

def get_last_block_file(blocks_dir):
    """Get the last block file name and modification time."""
    pattern = os.path.join(blocks_dir, "blk*.dat")
    files = glob.glob(pattern)
    if not files:
        return None, None
    
    files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    last_file = files[0]
    mtime = os.path.getmtime(last_file)
    filename = os.path.basename(last_file)
    return filename, mtime

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
        
        self.check_period_sec = int(os.getenv('CHECK_PERIOD_SEC', '5'))
        
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
            
            log_dir = os.getenv('BITCOIN2CLICKHOUSE_LOG_DIR', '/var/log/bitcoin2clickhouse')
            log_file = os.getenv('BITCOIN2CLICKHOUSE_LOG_FILE', os.path.join(log_dir, 'daemon.log'))
            
            try:
                os.makedirs(os.path.dirname(log_file), exist_ok=True)
                file_handler = logging.FileHandler(log_file)
                file_handler.setLevel(logging.INFO)
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)
            except PermissionError:
                self.logger.warning(f"Cannot write to {log_file}, using stdout only")
            
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            
            self.logger.setLevel(logging.INFO)
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
            self.logger.error(format_error_with_location(e, "Error initializing loader: "))
            return False
    
    def load_new_blocks(self, blockchain_path, xor_dat_path=None):
        try:
            xor_key = None
            if xor_dat_path:
                with open(xor_dat_path, 'rb') as f:
                    xor_key = f.read()
            
            block_files = sorted(glob.glob(os.path.join(blockchain_path, "blk*.dat")), reverse=True)
            
            start_file = None
            stored_hashes = []
            
            for blk_file_path in block_files:
                stored_hashes = self.loader.daemon_find_stored_blocks(blk_file_path, xor_key)
                if stored_hashes:
                    start_file = blk_file_path
                    break
            
            if start_file is None:
                self.logger.info("No stored blocks found - starting from first file")
                block_files = sorted(glob.glob(os.path.join(blockchain_path, "blk*.dat")))
                if block_files:
                    start_file = block_files[0]
                    stored_hashes = []
                else:
                    self.logger.warning("No block files found")
                    return True
            
            start_index = None
            block_files_sorted = sorted(glob.glob(os.path.join(blockchain_path, "blk*.dat")))
            for i, blk_file_path in enumerate(block_files_sorted):
                if blk_file_path == start_file:
                    start_index = i
                    break
            
            if start_index is None:
                self.logger.error(f"Start file {start_file} not found in sorted list")
                return False
            
            self.loader.daemon_load_new_blocks_from_file(start_file, stored_hashes, xor_key)
            
            for blk_file_path in block_files_sorted[start_index + 1:]:
                self.loader.daemon_load_new_blocks_from_file(blk_file_path, [], xor_key)
            
            return True
            
        except Exception as e:
            self.logger.error(format_error_with_location(e, "Error in load_new_blocks: "))
            raise e
    
    def run(self):
        self.logger.info("Bitcoin2ClickHouse Daemon starting...")
        self.logger.info(f"ClickHouse: {self.clickhouse_host}:{self.clickhouse_port}")
        self.logger.info(f"Database: {self.clickhouse_database}")
        self.logger.info(f"Bitcoin blocks: {self.bitcoin_blocks_dir}")
        if self.xor_dat_path:
            self.logger.info(f"XOR dat file: {self.xor_dat_path}")
        self.logger.info(f"Check period: {self.check_period_sec} seconds")
        self.logger.info("-" * 50)
        
        if not self._validate_environment():
            return 1
        
        if not self._initialize_loader():
            return 1
        
        self.logger.info("Daemon started. Monitoring for new blocks...")
        self.logger.info("Press Ctrl+C to stop")
        
        last_filename, last_mtime = None, None
        
        while self.running:
            
            try:
                
                current_filename, current_mtime = get_last_block_file(self.bitcoin_blocks_dir)
                
                if current_filename is None:
                    self.logger.warning("No block files found - skipping check")
                    continue
                
                if last_filename != current_filename or last_mtime != current_mtime:
                    last_filename = current_filename
                    last_mtime = current_mtime
                    
                    try:
                        self.load_new_blocks(self.bitcoin_blocks_dir, self.xor_dat_path)
                    except Exception as e:
                        self.logger.error(format_error_with_location(e, "Error in load_new_blocks: "))
                
                time.sleep(self.check_period_sec)
                
                if not self.running:
                    break

            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.error(format_error_with_location(e, "Unexpected error: "))
                time.sleep(self.check_period_sec)
        
        self.logger.info("Daemon stopped")
        return 0

def main():
    daemon = Bitcoin2ClickHouseDaemon()
    return daemon.run()

if __name__ == "__main__":
    sys.exit(main())

