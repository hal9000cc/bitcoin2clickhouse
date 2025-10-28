#!/usr/bin/env python3

import os
import sys
import time
import signal
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from clickhouse_driver.writer import MAX_INT64
from dotenv import load_dotenv
from blockchain_parser.blockchain import get_block
from blockchain_parser.block import Block
from src.bitcoin2clickhouse import BitcoinClickHouseLoader

def format_error_with_location(error, context=""):
    """Format error with file and line number information"""
    import traceback
    import inspect
    
    tb = traceback.extract_tb(error.__traceback__)
    if tb:
        frame = tb[-1]
        filename = frame.filename.split('/')[-1]  # Get just the filename
        line_number = frame.lineno
        function_name = frame.name
        return f"{context}{error} (File: {filename}, Line: {line_number}, Function: {function_name})"
    else:
        return f"{context}{error}"

def load_blocks_worker(blockchain_path, xor_dat_path, block_indexes, batch_size, worker_id, cache_file, clickhouse_host=None, clickhouse_port=None, clickhouse_user=None, clickhouse_password=None, database=None):
    """Worker process for loading blocks"""
    try:
        
        logger = logging.getLogger(f'worker_{worker_id}')
        if not logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            
            os.makedirs('logs', exist_ok=True)
            file_handler = logging.FileHandler(os.path.join('logs', f'bitcoin2clickhouse_worker_{worker_id}.log'))
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)
            
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            
            logger.setLevel(logging.INFO)
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
        
        worker_loader = BitcoinClickHouseLoader(
            clickhouse_host=clickhouse_host,
            clickhouse_port=clickhouse_port,
            clickhouse_user=clickhouse_user,
            clickhouse_password=clickhouse_password,
            database=database
        )
        
        logger.info(f"Worker {worker_id}: Loading {len(block_indexes)} blocks")
        
        xor_key = None
        if xor_dat_path:
            with open(xor_dat_path, 'rb') as f:
                xor_key = f.read()

        inputs_batch = []
        outputs_batch = []
        blocks_batch = []
        
        # target_heights = set(block_idx.height for block_idx in block_indexes)
        # min_height = min(block_idx.height for block_idx in block_indexes)
        # max_height = max(block_idx.height for block_idx in block_indexes)
        
        # logger.info(f"Worker {worker_id}: Looking for blocks {min_height}-{max_height}")
        
        for block_idx in block_indexes:
            try:
                blkFile = os.path.join(blockchain_path, f"blk{block_idx.file:05d}.dat")
                raw_block = get_block(blkFile, block_idx.data_pos, xor_key)
                block = Block(raw_block, block_idx.height)
                
                inputs_data, outputs_data, blocks_data = worker_loader.parse_block(block)
                
                if inputs_data is not None:
                    inputs_batch.extend(inputs_data)
                    outputs_batch.extend(outputs_data)
                    blocks_batch.append(blocks_data)
                    
                    if len(inputs_batch) >= batch_size or len(outputs_batch) >= batch_size:
                        worker_loader.insert_data(inputs_batch, outputs_batch, blocks_batch)
                        inputs_batch = []
                        outputs_batch = []
                        blocks_batch = []
                    
                    #logger.info(f"Worker {worker_id}: Processed block {block.height}: {block.hash}")
                
            except Exception as e:
                logger.error(format_error_with_location(e, f"Worker {worker_id}: Error processing block {block_idx.height}: "))
                raise e
        
        if inputs_batch or outputs_batch or blocks_batch:
            worker_loader.insert_data(inputs_batch, outputs_batch, blocks_batch)
        
        return f"Worker {worker_id}: Successfully loaded {len(block_indexes)} blocks"
        
    except Exception as e:
        return f"Worker {worker_id}: Failed with error: {e}"

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
        self.num_workers = int(os.getenv('NUM_WORKERS', '4'))
        
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
            
            os.makedirs('logs', exist_ok=True)
            file_handler = logging.FileHandler(os.path.join('logs', 'bitcoin2clickhouse_daemon.log'))
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
            self.logger.error(format_error_with_location(e, "Error initializing loader: "))
            return False
    
    def load_new_blocks(self, max_block):
        self.logger.info(f"Loading new blocks from {self.bitcoin_blocks_dir}...")
        
        cache_file = '/tmp/bitcoin_index_shared_cache.pkl'
        
        try:
            unloaded_blocks = self.loader.unloaded_blocks(self.bitcoin_blocks_dir, self.xor_dat_path, max_block)
            
            if not unloaded_blocks:
                self.logger.info("No unloaded blocks found")
                return True
            
            self.logger.info(f"Found {len(unloaded_blocks)} unloaded blocks")
            
            unloaded_blocks.sort(key=lambda x: x.height)
            self.logger.info(f"Sorted blocks from {min(block_idx.height for block_idx in unloaded_blocks)} to {max(block_idx.height for block_idx in unloaded_blocks)}")
            
            max_chunk_size = 1008
            remaining_blocks = unloaded_blocks.copy()
            completed_rounds = 0
            
            while remaining_blocks and self.running:
                completed_rounds += 1
                self.logger.info(f"Starting round {completed_rounds} with {len(remaining_blocks)} remaining blocks")
                
                chunk_size = min(max_chunk_size, len(remaining_blocks) // self.num_workers)
                if chunk_size == 0:
                    chunk_size = 1
                
                block_chunks = []
                for i in range(0, min(len(remaining_blocks), self.num_workers * chunk_size), chunk_size):
                    chunk = remaining_blocks[i:i + chunk_size]
                    if chunk:
                        block_chunks.append(chunk)
                
                self.logger.info(f"Round {completed_rounds}: Split into {len(block_chunks)} chunks (max {chunk_size} blocks each)")
                for i, chunk in enumerate(block_chunks):
                    self.logger.info(f"Chunk {i+1}: {len(chunk)} blocks (range: {min(block_idx.height for block_idx in chunk)}-{max(block_idx.height for block_idx in chunk)})")
                
                with ProcessPoolExecutor(max_workers=min(len(block_chunks), self.num_workers)) as executor:
                    futures = []
                    
                    for i, chunk in enumerate(block_chunks):
                        self.logger.info(f"Starting worker {i+1}: {len(chunk)} blocks")
                        
                        future = executor.submit(
                            load_blocks_worker,
                            self.bitcoin_blocks_dir,
                            self.xor_dat_path,
                            chunk,
                            1000000,
                            i+1,
                            cache_file,
                            self.clickhouse_host,
                            self.clickhouse_port,
                            self.clickhouse_user,
                            self.clickhouse_password,
                            self.clickhouse_database
                        )
                        futures.append(future)
                    
                    completed_workers = 0
                    processed_blocks = set()
                    
                    for future in as_completed(futures):
                        if not self.running:
                            self.logger.info("Stopping parallel loading due to interrupt signal")
                            for f in futures:
                                f.cancel()
                            break
                            
                        try:
                            result = future.result()
                            completed_workers += 1
                            self.logger.info(f"Worker {completed_workers} completed: {result}")
                            
                            for chunk in block_chunks:
                                for block_idx in chunk:
                                    processed_blocks.add(block_idx.height)
                            
                        except Exception as e:
                            self.logger.error(format_error_with_location(e, "Worker failed: "))
                    
                    if not self.running:
                        break
                    
                    remaining_blocks = [block_idx for block_idx in remaining_blocks if block_idx.height not in processed_blocks]
                    self.logger.info(f"Round {completed_rounds} completed. {len(remaining_blocks)} blocks remaining")
            
            self.logger.info("Parallel loading completed")
            return True
            
        except Exception as e:
            self.logger.error(format_error_with_location(e, "Error in parallel loading: "))
            return False
    
    def run(self, max_block):
        self.logger.info("Bitcoin2ClickHouse Daemon starting...")
        self.logger.info(f"ClickHouse: {self.clickhouse_host}:{self.clickhouse_port}")
        self.logger.info(f"Database: {self.clickhouse_database}")
        self.logger.info(f"Bitcoin blocks: {self.bitcoin_blocks_dir}")
        if self.xor_dat_path:
            self.logger.info(f"XOR dat file: {self.xor_dat_path}")
        self.logger.info(f"Delay: {self.delay_minutes} minutes")
        self.logger.info(f"Workers: {self.num_workers}")
        self.logger.info("-" * 50)
        
        if not self._validate_environment():
            return 1
        
        if not self._initialize_loader():
            return 1
        
        self.logger.info(f"Starting daemon with {self.delay_minutes} minute intervals...")
        self.logger.info("Press Ctrl+C to stop")
        
        while self.running:
            try:
                success = self.load_new_blocks(max_block)
                if not success:
                    self.logger.warning("Failed to load new blocks, will retry on next cycle")
                else:
                    self.logger.info("All blocks processed successfully")
                    break
                
                if self.running:
                    self.logger.info(f"Waiting {self.delay_minutes} minutes before next check...")
                    for _ in range(self.delay_minutes * 60):
                        if not self.running:
                            break
                        time.sleep(1)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.error(format_error_with_location(e, "Unexpected error: "))
                raise e
        
        self.logger.info("Daemon stopped")
        return 0

def main():
    daemon = Bitcoin2ClickHouseDaemon()
    return daemon.run(int(os.getenv('MAX_BLOCK', MAX_INT64)))

if __name__ == "__main__":
    sys.exit(main())
