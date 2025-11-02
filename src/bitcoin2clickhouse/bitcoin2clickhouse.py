import sys
import os
import logging
import glob
from datetime import datetime
from clickhouse_driver import Client
from clickhouse_driver.writer import MAX_INT64
from blockchain_parser.blockchain import Blockchain
from blockchain_parser.blockchain import get_block, get_blocks
from blockchain_parser.block import Block

def format_error_with_location(error, context=""):
    """Format error with file and line number information"""
    import traceback
    
    tb = traceback.extract_tb(error.__traceback__)
    if tb:
        frame = tb[-1]
        filename = frame.filename.split('/')[-1]  # Get just the filename
        line_number = frame.lineno
        function_name = frame.name
        return f"{context}{error} (File: {filename}, Line: {line_number}, Function: {function_name})"
    else:
        return f"{context}{error}"

class BitcoinClickHouseLoader:
    def __init__(self, clickhouse_host='localhost', clickhouse_port=9000, 
                 clickhouse_user='default', clickhouse_password='', database='default',
                 update_batch_size=None):
        """Initialize ClickHouse connection"""
        self.logger = logging.getLogger(__name__)
        self._setup_logging()
        self.database = database
        self.clickhouse_host = clickhouse_host
        self.clickhouse_port = clickhouse_port
        self.clickhouse_user = clickhouse_user
        self.clickhouse_password = clickhouse_password
        
        if update_batch_size is None:
            update_batch_size = int(os.getenv('UPDATE_BATCH_SIZE', '10000'))
        self.update_batch_size = update_batch_size
        
        self.client = self.database_connect()
    
    def _setup_logging(self):
        if not self.logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            
            os.makedirs('logs', exist_ok=True)
            file_handler = logging.FileHandler(os.path.join('logs', 'bitcoin2clickhouse.log'))
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)
            
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            
            self.logger.setLevel(logging.INFO)
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
    
    def database_connect(self):
        try:
            default_client = Client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                user=self.clickhouse_user,
                password=self.clickhouse_password,
                database='default'
            )
            
            default_client.execute(f'CREATE DATABASE IF NOT EXISTS {self.database}')
            
            client = Client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                user=self.clickhouse_user,
                password=self.clickhouse_password,
                database=self.database
            )
            self.logger.debug(f"Connected to database {self.database}")
            
            self.database_initialize(client)
            
            return client
                
        except Exception as e:
            self.logger.error(format_error_with_location(e, "Error connecting to database: "))
            raise
        
    def database_initialize(self, client):
        if self.database_initialized(client):
            return
            
        self.logger.info("Initializing database schema...")
        
        schema_path = os.path.join(os.path.dirname(__file__), 'clickhouse_schema.sql')
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
        
        lines = schema_sql.split('\n')
        current_query = ""
        queries = []
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('--'):
                continue
            
            if '--' in line:
                line = line.split('--')[0].strip()
                if not line:
                    continue
            
            current_query += line + " "
            
            if line.endswith(';'):
                if current_query.strip() and len(current_query.strip()) > 10:
                    queries.append(current_query.strip())
                current_query = ""
        
        self.logger.info(f"Found {len(queries)} SQL queries")
        
        for query in queries:
            try:
                client.execute(query)
            except Exception as e:
                self.logger.error(format_error_with_location(e, "Error executing query: "))
                self.logger.error(f"Problematic query: {query}")
                raise e
        
        self.logger.info("Database initialization completed")
    
    def database_initialized(self, client):

        try:
            result = client.execute(
                "SHOW TABLES WHERE name = 'db_version'"
            )
            return len(result) > 0
        except Exception as e:
            self.logger.error(format_error_with_location(e, "Error checking database initialization: "))
            raise e
    
    def parse_block(self, block):

        inputs_data = []
        outputs_data = []
        block_data = None
        
        block_timestamp = block.header.timestamp
        
        for tx_index, transaction in enumerate(block.transactions):
            for input_index, input_tx in enumerate(transaction.inputs):
                input_data = {
                    'n_block': block.height,
                    'tx_id': self._hex2hash32(transaction.txid),
                    'input_index': input_index,
                    'prev_tx_hash': self._hex2hash32(input_tx.transaction_hash),
                    'prev_tx_index': input_tx.transaction_index,
                    'sequence_number': input_tx.sequence_number,
                    'script_hex': input_tx.script.hex,
                    'script_type': self._get_script_type(input_tx.script),
                    'is_segwit': 1 if transaction.is_segwit else 0,
                    'witness_count': len(input_tx.witnesses),
                    'witness_data': [w.hex() for w in input_tx.witnesses],
                    'input_size': input_tx.size,
                    'is_coinbase': 1 if transaction.is_coinbase() else 0
                }
                inputs_data.append(input_data)
            
            for output_index, output_tx in enumerate(transaction.outputs):
                addresses = output_tx.addresses
                address_types = [addr.type for addr in addresses] if addresses else []
                
                output_data = {
                    'n_block': block.height,
                    'tx_id': self._hex2hash32(transaction.txid),
                    'output_index': output_index,
                    'value': output_tx.value,
                    'script_hex': output_tx.script.hex,
                    'script_type': self._get_output_script_type(output_tx),
                    'is_p2pkh': 1 if output_tx.is_pubkeyhash() else 0,
                    'is_p2sh': 1 if output_tx.is_p2sh() else 0,
                    'is_p2wpkh': 1 if output_tx.is_p2wpkh() else 0,
                    'is_p2wsh': 1 if output_tx.is_p2wsh() else 0,
                    'is_p2tr': 1 if hasattr(output_tx, 'is_p2tr') and output_tx.is_p2tr() else 0,
                    'is_multisig': 1 if output_tx.is_multisig() else 0,
                    'is_unknown': 1 if output_tx.is_unknown() else 0,
                    'is_op_return': 1 if output_tx.is_return() else 0,
                    'address_count': len(addresses),
                    'addresses': [addr.address for addr in addresses] if addresses else [],
                    'address_types': address_types,
                    'created_at': datetime.now()
                }
                outputs_data.append(output_data)
        
        block_data = {
            'block_hash': self._hex2hash32(block.hash),
            'n_block': block.height,
            'block_timestamp': block_timestamp,
            'version': block.header.version,
            'prev_block_hash': self._hex2hash32(block.header.previous_block_hash),
            'merkle_root': self._hex2hash32(block.header.merkle_root),
            'nonce': block.header.nonce,
            'bits': block.header.bits,
            'size': block.size,
            'weight': getattr(block, 'weight', 0),
            'transaction_count': len(block.transactions),
            'processed_at': datetime.now()
        }
        
        return inputs_data, outputs_data, block_data
    
    @staticmethod
    def _hex2hash32(hex_string):
        """Convert hex string to bytes for FixedString(32)"""
        if not hex_string:
            return b'\x00' * 32
        return bytes.fromhex(hex_string)
    
    @staticmethod
    def _to_hex(value):
        if value is None:
            return ''
        if hasattr(value, 'hex'):
            try:
                return value.hex()
            except Exception:
                pass
        if isinstance(value, str):
            try:
                return value.encode('latin1').hex()
            except Exception:
                return value
        try:
            return bytes(value).hex()
        except Exception:
            return str(value)
    
    
    def _get_script_type(self, script):
        if script.is_pubkey():
            return 'pubkey'
        elif script.is_pubkeyhash():
            return 'pubkeyhash'
        elif script.is_p2sh():
            return 'p2sh'
        elif script.is_multisig():
            return 'multisig'
        elif script.is_p2wpkh():
            return 'p2wpkh'
        elif script.is_p2wsh():
            return 'p2wsh'
        else:
            return 'unknown'
    
    def _get_output_script_type(self, output):
        if output.is_pubkey():
            return 'pubkey'
        elif output.is_pubkeyhash():
            return 'pubkeyhash'
        elif output.is_p2sh():
            return 'p2sh'
        elif output.is_multisig():
            return 'multisig'
        elif output.is_p2wpkh():
            return 'p2wpkh'
        elif output.is_p2wsh():
            return 'p2wsh'
        elif hasattr(output, 'is_p2tr') and output.is_p2tr():
            return 'p2tr'
        elif output.is_return():
            return 'op_return'
        else:
            return 'unknown'
    
    def insert_data(self, inputs_data, outputs_data, blocks_data):

        if inputs_data:
            self.client.execute(
                'INSERT INTO tran_in VALUES',
                inputs_data
            )
            self.logger.debug(f"Inserted {len(inputs_data)} inputs")
        
        if outputs_data:
            self.client.execute(
                'INSERT INTO tran_out VALUES',
                outputs_data
            )
            self.logger.debug(f"Inserted {len(outputs_data)} outputs")
        
        if blocks_data:
            self.client.execute(
                'INSERT INTO blocks VALUES',
                blocks_data
            )
            block_info = [(block['n_block'], block['block_timestamp'].strftime('%Y-%m-%d %H:%M:%S')) for block in blocks_data]
            self.logger.info(f"Inserted {len(blocks_data)} blocks: {block_info}")
            self.update_all()
    
    def update_all(self):
        try:
            initial_result = self.client.execute("SELECT count() FROM blocks_mod")
            remaining_count = initial_result[0][0] if initial_result else 0
            
            if remaining_count == 0:
                return
            
            while True:
                result = self.client.execute(
                    f"SELECT n_block FROM blocks_mod ORDER BY n_block LIMIT {self.update_batch_size}"
                )
                if not result or len(result) == 0:
                    break
                
                block_numbers = [row[0] for row in result]
                self.logger.debug(f"Processing batch of {len(block_numbers)} blocks")
                
                self.update_turnover(block_numbers)
                self.update_turnover_m(block_numbers)
                
                block_numbers_str = ','.join(map(str, block_numbers))
                self.client.execute(f"DELETE FROM blocks_mod WHERE n_block IN ({block_numbers_str})")
                
                remaining_count -= len(block_numbers)
                
                self.logger.info(f"Processed and deleted {len(block_numbers)} blocks from blocks_mod, {remaining_count} remaining")
        except KeyboardInterrupt:
            self.logger.info("Update interrupted by user (Ctrl+C)")
            raise
        except Exception as e:
            self.logger.error(format_error_with_location(e, f"Error in update_all: "))
    
    def update_turnover(self, block_numbers):
        try:
            if not block_numbers:
                return
            
            block_numbers_str = ','.join(map(str, block_numbers))
            
            self.client.execute(f"""
                INSERT INTO turnover (time, tx_id, address, value)
                SELECT 
                    b.block_timestamp as time,
                    to.tx_id as tx_id,
                    to.addresses[1] as address,
                    sum(to.value / 100000000.0) as value 
                FROM blocks b
                JOIN tran_out to ON to.n_block = b.n_block
                WHERE b.n_block IN ({block_numbers_str})
                  AND to.address_count > 0
                GROUP BY time, tx_id, address
            """)
            
            self.client.execute(f"""
                INSERT INTO turnover (time, tx_id, address, value)
                    WITH needed_tx_ids AS (
                        SELECT DISTINCT prev_tx_hash
                        FROM tran_in 
                        WHERE n_block IN ({block_numbers_str})
                    )
                    SELECT 
                        b.block_timestamp as time,
                        ti.tx_id as tx_id,
                        to.addresses[1] as address,
                        -sum(to.value / 100000000.0) as value  
                    FROM blocks b
                    JOIN tran_in ti ON ti.n_block = b.n_block
                    JOIN tran_out to ON 
                        ti.prev_tx_hash = to.tx_id 
                        AND ti.input_index = to.output_index 
                        AND to.address_count > 0
                        AND to.tx_id IN needed_tx_ids
                    WHERE b.n_block IN ({block_numbers_str})
                    GROUP BY time, tx_id, address
            """)
            
            self.logger.debug(f"Updated turnover for {len(block_numbers)} blocks")
        except Exception as e:
            self.logger.error(format_error_with_location(e, f"Error in update_turnover: "))
    
    def update_turnover_m(self, block_numbers):
        try:
            if not block_numbers:
                return
            
            block_numbers_str = ','.join(map(str, block_numbers))
            
            self.client.execute(f"""
                INSERT INTO turnover_m (time_month, address, value, updated_at)
                SELECT 
                    toStartOfMonth(time) as time_month,
                    address,
                    sum(value) as value,
                    now() as updated_at
                FROM turnover
                WHERE toStartOfMonth(time) >= (
                    SELECT toStartOfMonth(min(block_timestamp)) 
                    FROM blocks 
                    WHERE n_block IN ({block_numbers_str})
                )
                  AND toStartOfMonth(time) < (
                    SELECT toStartOfMonth(max(block_timestamp)) + INTERVAL 1 MONTH 
                    FROM blocks 
                    WHERE n_block IN ({block_numbers_str})
                  )
                GROUP BY time_month, address
            """)
            
            self.logger.debug(f"Updated turnover_m for {len(block_numbers)} blocks")
        except Exception as e:
            self.logger.error(format_error_with_location(e, f"Error in update_turnover_m: "))
    
    def last_stored_block(self):

        try:
            result = self.client.execute(
                'SELECT count() as total_count, MAX(n_block) as last_height FROM blocks'
            )
            if result and result[0][0] == 0:
                return -1
            if result and result[0][1] is not None and result[0][1] > 0:
                return result[0][1]
            return -1
        except Exception as e:
            self.logger.error(f"Error getting last processed block: {e}")
            raise e
    
    def load(self, blockchain_path, xor_dat_path=None, start_height=0, end_height=None, batch_size=500000, cache_file=None):
        """Load specified range of blocks from blockchain"""
        
        if xor_dat_path:
            blockchain = Blockchain(blockchain_path, xor_dat_path)
        else:
            blockchain = Blockchain(blockchain_path)

        self.logger.info(f"Loading blocks from {start_height} to {end_height or 'latest'}")

        inputs_batch = []
        outputs_batch = []
        blocks_batch = []
        spent_updates_batch = []
        
        for block in blockchain.get_ordered_blocks(
            blockchain_path + '/index',
            start=start_height,
            end=end_height,
            cache=cache_file
        ):
            try:
                inputs_data, outputs_data, block_data, spent_updates = self.parse_block(block)
                
                if inputs_data is not None:
                    inputs_batch.extend(inputs_data)
                    outputs_batch.extend(outputs_data)
                    blocks_batch.append(block_data)
                    spent_updates_batch.extend(spent_updates)
                    
                    if len(inputs_batch) >= batch_size or len(outputs_batch) >= batch_size:
                        self.insert_data(inputs_batch, outputs_batch, blocks_batch)
                        self.update_spent_outputs(spent_updates_batch)
                        inputs_batch = []
                        outputs_batch = []
                        blocks_batch = []
                        spent_updates_batch = []
                    
                    #self.logger.info(f"Processed block {block.height}: {block.hash}")
                
            except Exception as e:
                self.logger.error(format_error_with_location(e, f"Error processing block {block.height}: "))
                continue
        
        if inputs_batch or outputs_batch or blocks_batch:
            self.insert_data(inputs_batch, outputs_batch, blocks_batch)
    
        if spent_updates_batch:
            self.update_spent_outputs(spent_updates_batch)
    
    def load_new(self, blockchain_path, xor_dat_path=None, batch_size=500000, cache_file=None):
        """Load new blocks from blockchain (determines range and calls load)"""
        
        last_stored = self.last_stored_block()
        if last_stored >= 0:
            actual_start = last_stored + 1
            self.logger.debug(f"Last stored block: {last_stored}")
        else:
            actual_start = 0
            
        self.logger.debug(f"Loading new blocks from {actual_start}")
        
        self.load(blockchain_path, xor_dat_path, actual_start, None, batch_size, cache_file)
    
    def unloaded_blocks(self, blockchain_path, xor_dat_path=None, end_height=MAX_INT64):
        """Get list of all unloaded blocks. Returns list of block heights."""
        
        last_loaded = self.last_stored_block()
        start_height = last_loaded + 1 if last_loaded >= 0 else 0
        
        self.logger.debug(f"Getting unloaded blocks from height {start_height}")
        
        if xor_dat_path:
            blockchain = Blockchain(blockchain_path, xor_dat_path)
        else:
            blockchain = Blockchain(blockchain_path)
        
        unloaded_blocks = []
        
        block_indexes = blockchain._Blockchain__getBlockIndexes(blockchain_path + '/index')
        
        if not block_indexes:
            raise Exception("No block indexes found")
        
        unloaded_blocks = [block_idx for block_idx in block_indexes if block_idx.height >= start_height and block_idx.height <= end_height]
        
        missing_blocks = []
        try:
            result = self.client.execute("""
                SELECT b1.n_block - 1
                FROM blocks b1
                LEFT JOIN blocks b2 ON b1.prev_block_hash = b2.block_hash
                WHERE b1.n_block > 0 AND b2.block_hash = ''
            """)
            
            missing_block_numbers = [row[0] for row in result]
            
            if missing_block_numbers:
                self.logger.info(f"Found {len(missing_block_numbers)} missing block chains")
                
                for missing_end_block in missing_block_numbers:
                    max_existing_result = self.client.execute("""
                        SELECT MAX(n_block)
                        FROM blocks
                        WHERE n_block <= %(missing_end_block)s
                    """, {'missing_end_block': missing_end_block})
                    
                    last_existing_block = max_existing_result[0][0] if max_existing_result[0][0] is not None else -1
                    
                    start_range = last_existing_block + 1
                    end_range = missing_end_block
                    
                    if start_range <= end_range:
                        self.logger.info(f"Adding missing blocks from {start_range} to {end_range}")
                        missing_indexes = [block_idx for block_idx in block_indexes 
                                         if start_range <= block_idx.height <= end_range]
                        unloaded_blocks.extend(missing_indexes)
                
        except Exception as e:
            self.logger.error(f"Error checking for missing blocks: {e}")
            raise e
        
        seen_heights = set()
        unique_blocks = []
        for block_idx in unloaded_blocks:
            if block_idx.height not in seen_heights:
                seen_heights.add(block_idx.height)
                unique_blocks.append(block_idx)
        
        unique_blocks.sort(key=lambda x: x.height)
        
        self.logger.info(f"Found {len(unique_blocks)} total unloaded blocks")
        return unique_blocks

    def check_database_integrity(self):
        """Check database integrity. Returns list of errors."""
        errors = []
        
        self.logger.info("Starting database integrity check...")
        
        try:
            errors.extend(self._check_genesis_block(check_number=1))
            errors.extend(self._check_block_sequence(check_number=2))
            errors.extend(self._check_transaction_count_consistency(check_number=3))
            errors.extend(self._check_n_block_consistency(check_number=4))

            if errors:
                self.logger.error(f"Check 1-4 completed. Next steps skipped. Found {len(errors)} errors.")
                return errors

            errors.extend(self._check_prev_block_hash_consistency(check_number=5))
            errors.extend(self._check_transaction_hash_consistency(check_number=6))
            errors.extend(self._check_prev_tx_hash_consistency(check_number=7))
            
            if errors:
                self.logger.error(f"Check completed. Found {len(errors)} errors.")
            else:
                self.logger.info("✓ All checks passed successfully. No errors found.")
            
            return errors
            
        except Exception as e:
            error = f"Error during database check: {e}"
            errors.append(error)
            self.logger.error(error)
            return errors

    def _check_genesis_block(self, check_number: int):
        """Block with number 0 exists. Returns list of errors."""
        errors = []
        
        self.logger.info(f"Check {check_number}: Existence of block with number 0...")
        try:
            result = self.client.execute(
                "SELECT count() FROM blocks WHERE n_block = 0"
            )
            if result[0][0] == 0:
                error = "Error: Block with number 0 not found in blocks table"
                errors.append(error)
                self.logger.error(error)
            else:
                self.logger.info("✓ Block with number 0 found")
        except Exception as e:
            error = f"Error in check {check_number} (genesis block): {e}"
            errors.append(error)
            self.logger.error(error)
        
        return errors

    def _check_n_block_consistency(self, check_number: int):
        """n_block consistency between tables. Returns list of errors."""
        errors = []
        
        self.logger.info(f"Check {check_number}: n_block consistency between tables...")
        
        try:
            result = self.client.execute("""
                SELECT DISTINCT ti.n_block
                FROM tran_in ti
                LEFT ANTI JOIN blocks b ON ti.n_block = b.n_block
            """)
            
            missing_in_blocks_from_tran_in = [row[0] for row in result]
            if missing_in_blocks_from_tran_in:
                error = f"Error: n_block in tran_in missing in blocks: {sorted(missing_in_blocks_from_tran_in)}"
                errors.append(error)
                self.logger.error(error)
            
            result = self.client.execute("""
                SELECT DISTINCT b.n_block
                FROM blocks b
                LEFT ANTI JOIN tran_in ti ON b.n_block = ti.n_block
            """)
            
            missing_in_tran_in_from_blocks = [row[0] for row in result]
            if missing_in_tran_in_from_blocks:
                error = f"Error: n_block in blocks missing in tran_in: {sorted(missing_in_tran_in_from_blocks)}"
                errors.append(error)
                self.logger.error(error)
            
            result = self.client.execute("""
                SELECT DISTINCT to.n_block
                FROM tran_out to
                LEFT ANTI JOIN blocks b ON to.n_block = b.n_block
            """)
            
            missing_in_blocks_from_tran_out = [row[0] for row in result]
            if missing_in_blocks_from_tran_out:
                error = f"Error: n_block in tran_out missing in blocks: {sorted(missing_in_blocks_from_tran_out)}"
                errors.append(error)
                self.logger.error(error)
            
            result = self.client.execute("""
                SELECT DISTINCT b.n_block
                FROM blocks b
                LEFT ANTI JOIN tran_out to ON b.n_block = to.n_block
            """)
            
            missing_in_tran_out_from_blocks = [row[0] for row in result]
            if missing_in_tran_out_from_blocks:
                error = f"Error: n_block in blocks missing in tran_out: {sorted(missing_in_tran_out_from_blocks)}"
                errors.append(error)
                self.logger.error(error)
            
            if not missing_in_blocks_from_tran_in and not missing_in_tran_in_from_blocks and not missing_in_blocks_from_tran_out and not missing_in_tran_out_from_blocks:
                self.logger.info("✓ n_block consistency between tables is correct")
        
        except Exception as e:
            error = f"Error in check {check_number} (n_block consistency): {e}"
            errors.append(error)
            self.logger.error(error)
        
        return errors

    def _check_prev_block_hash_consistency(self, check_number: int):
        """prev_block_hash consistency. Returns list of errors."""
        errors = []
        
        self.logger.info(f"Check {check_number}: prev_block_hash consistency...")
        
        try:
            result = self.client.execute("""
                SELECT b1.n_block, b1.block_hash, b1.prev_block_hash, b2.block_hash as prev_hash
                FROM blocks b1
                LEFT JOIN blocks b2 ON b1.prev_block_hash = b2.block_hash AND b1.n_block = b2.n_block + 1
                WHERE b1.n_block > 0 AND b2.block_hash = ''
            """)
            
            prev_hash_errors = [f"Error: For block {row[0]} (hash: {row[1]}) previous block with hash {row[2]} not found" for row in result]
            
            if prev_hash_errors:
                errors.extend(prev_hash_errors)
                for error in prev_hash_errors:
                    self.logger.error(error)
            else:
                self.logger.info("✓ prev_block_hash consistency is correct")
        
        except Exception as e:
            error = f"Error in check {check_number} (prev_block_hash consistency): {e}"
            errors.append(error)
            self.logger.error(error)
        
        return errors

    def _check_transaction_hash_consistency(self, check_number: int):
        """transaction_hash consistency between tran_in and tran_out. Returns list of errors."""
        errors = []
        block_batch_size = 100000
        max_display_errors = 100
        
        self.logger.info(f"Check {check_number}: transaction_hash consistency between tran_in and tran_out...")
        
        try:
            max_block_result = self.client.execute("SELECT MAX(n_block) FROM tran_in")
            max_block = max_block_result[0][0] if max_block_result[0][0] is not None else 0
            
            missing_in_tran_out = []
            missing_in_tran_in = []
            
            start_block = 0
            while start_block <= max_block:
                end_block = min(start_block + block_batch_size, max_block + 1)
                
                result = self.client.execute("""
                    SELECT DISTINCT hex(ti.tx_id) as tx_hash
                    FROM tran_in ti 
                    LEFT ANTI JOIN tran_out to
                        ON ti.n_block = to.n_block 
                        AND ti.tx_id = to.tx_id 
                        AND ti.tx_id != ''
                    WHERE ti.n_block >= %(start_block)s AND ti.n_block < %(end_block)s
                    LIMIT %(max_errors)s
                """, {
                    'start_block': start_block,
                    'end_block': end_block,
                    'max_errors': max_display_errors - len(missing_in_tran_out)
                })
                
                if result:
                    missing_in_tran_out.extend([row[0] for row in result])
                
                if len(missing_in_tran_out) >= max_display_errors:
                    break
                
                result = self.client.execute("""
                    SELECT DISTINCT hex(to.tx_id) as tx_hash
                    FROM tran_out to 
                    LEFT ANTI JOIN tran_in ti
                        ON to.n_block = ti.n_block 
                        AND to.tx_id = ti.tx_id 
                        AND to.tx_id != ''
                    WHERE to.n_block >= %(start_block)s AND to.n_block < %(end_block)s
                    LIMIT %(max_errors)s
                """, {
                    'start_block': start_block,
                    'end_block': end_block,
                    'max_errors': max_display_errors - len(missing_in_tran_in)
                })
                
                if result:
                    missing_in_tran_in.extend([row[0] for row in result])
                
                if len(missing_in_tran_in) >= max_display_errors:
                    break
                
                self.logger.info(f"Processed blocks {start_block} to {end_block - 1}, found {len(missing_in_tran_out)} missing in tran_out, {len(missing_in_tran_in)} missing in tran_in...")
                
                start_block = end_block
            
            if missing_in_tran_out:
                error = f"Error: Transactions in tran_in missing in tran_out (showing first {len(missing_in_tran_out)}): {missing_in_tran_out[:max_display_errors]}"
                errors.append(error)
                self.logger.error(error)
            
            if missing_in_tran_in:
                error = f"Error: Transactions in tran_out missing in tran_in (showing first {len(missing_in_tran_in)}): {missing_in_tran_in[:max_display_errors]}"
                errors.append(error)
                self.logger.error(error)
            
            if not missing_in_tran_out and not missing_in_tran_in:
                self.logger.info("✓ transaction_hash consistency between tran_in and tran_out is correct")
        
        except Exception as e:
            error = f"Error in check {check_number} (transaction_hash consistency): {e}"
            errors.append(error)
            self.logger.error(error)
        
        return errors

    def _check_prev_tx_hash_consistency(self, check_number: int):
        """prev_tx_hash from tran_in matches transactions in tran_out. Returns list of errors."""

        errors = []
        
        self.logger.info(f"Check {check_number}: prev_tx_hash from tran_in matches transactions in tran_out...")
        
        try:
            result = self.client.execute("""
                SELECT ti.prev_tx_hash, ti.tx_id, ti.input_index
                FROM tran_in ti LEFT ANTI JOIN tran_out to
                    ON ti.prev_tx_hash = to.tx_id
                WHERE ti.prev_tx_hash != ''
                LIMIT 100
                SETTINGS
                    join_algorithm = 'grace_hash',
                    grace_hash_join_initial_buckets = 8
            """)
            
            if result:
                display_errors = [
                    f"{self._to_hex(row[0])} (in transaction {self._to_hex(row[1])}, input {row[2]})"
                    for row in result
                ]
                error = f"Error: prev_tx_hash from tran_in missing in tran_out: {display_errors}"
                errors.append(error)
                self.logger.error(error)
            else:
                self.logger.info("✓ prev_tx_hash consistency is correct")
        
        except Exception as e:
            error = f"Error in check {check_number} (prev_tx_hash consistency): {e}"
            errors.append(error)
            self.logger.error(error)
        
        return errors
    
    def _check_transaction_count_consistency(self, check_number: int):
        """transaction_count matches actual transaction count in tran_in/tran_out. Returns list of errors."""
        errors = []
        
        self.logger.info(f"Check {check_number}: transaction_count consistency...")
        
        try:
            result = self.client.execute("""
                SELECT b.n_block, MIN(b.transaction_count), COUNT(DISTINCT ti.tx_id) as actual_count
                FROM blocks b
                LEFT JOIN tran_in ti ON b.n_block = ti.n_block
                GROUP BY b.n_block
                HAVING MIN(b.transaction_count) != actual_count
            """)
            
            inconsistent_counts_in = [f"Block {row[0]}: declared {row[1]} transactions, actual in tran_in {row[2]}" for row in result]
            
            if inconsistent_counts_in:
                error = f"Error: transaction_count inconsistency in tran_in: {inconsistent_counts_in}"
                errors.append(error)
                self.logger.error(error)
            
            result = self.client.execute("""
                SELECT b.n_block, MIN(b.transaction_count), COUNT(DISTINCT to.tx_id) as actual_count
                FROM blocks b
                LEFT JOIN tran_out to ON b.n_block = to.n_block
                GROUP BY b.n_block
                HAVING MIN(b.transaction_count) != actual_count
            """)
            
            inconsistent_counts_out = [f"Block {row[0]}: declared {row[1]} transactions, actual in tran_out {row[2]}" for row in result]
            
            if inconsistent_counts_out:
                error = f"Error: transaction_count inconsistency in tran_out: {inconsistent_counts_out}"
                errors.append(error)
                self.logger.error(error)
            
            if not inconsistent_counts_in and not inconsistent_counts_out:
                self.logger.info("✓ transaction_count consistency is correct")
        
        except Exception as e:
            error = f"Error in check {check_number} (transaction_count consistency): {e}"
            errors.append(error)
            self.logger.error(error)
        
        return errors
    
    def _check_block_sequence(self, check_number: int):
        """n_block sequence without gaps. Returns list of errors."""
        errors = []
        
        self.logger.info(f"Check {check_number}: block sequence without gaps...")
        
        try:
            result = self.client.execute("""
                SELECT n_block
                FROM blocks
                WHERE n_block > 0
                ORDER BY n_block
            """)
            
            if not result:
                self.logger.info("✓ No blocks to check sequence")
                return errors
            
            block_numbers = [row[0] for row in result]
            max_block = max(block_numbers)
            
            expected_sequence = set(range(1, max_block + 1))
            actual_sequence = set(block_numbers)
            
            missing_blocks = sorted(expected_sequence - actual_sequence)
            
            if missing_blocks:
                error = f"Error: Missing blocks in sequence: {missing_blocks}"
                errors.append(error)
                self.logger.error(error)
            else:
                self.logger.info("✓ Block sequence is complete")
        
        except Exception as e:
            error = f"Error in check {check_number} (block sequence): {e}"
            errors.append(error)
            self.logger.error(error)
        
        return errors
    
    @staticmethod
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
            
            logger.debug(f"Worker {worker_id}: Loading {len(block_indexes)} blocks")
            
            xor_key = None
            if xor_dat_path:
                with open(xor_dat_path, 'rb') as f:
                    xor_key = f.read()

            inputs_batch = []
            outputs_batch = []
            blocks_batch = []
                    
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
                        
                except Exception as e:
                    logger.error(format_error_with_location(e, f"Worker {worker_id}: Error processing block {block_idx.height}: "))
                    raise e
            
            if inputs_batch or outputs_batch or blocks_batch:
                worker_loader.insert_data(inputs_batch, outputs_batch, blocks_batch)
            
            return f"Worker {worker_id}: Successfully loaded {len(block_indexes)} blocks"
            
        except Exception as e:
            return f"Worker {worker_id}: Failed with error: {e}"
    
    def daemon_find_stored_blocks(self, blockfile_path, xor_key=None):
        try:
            block_hashes = []
            for block_raw in get_blocks(blockfile_path, xor_key):
                block = Block(block_raw, None)
                block_hash = self._hex2hash32(block.hash)
                block_hashes.append(block_hash)
            
            if not block_hashes:
                return []
            
            stored_hashes = []
            batch_size = 1000
            for i in range(0, len(block_hashes), batch_size):
                batch = block_hashes[i:i + batch_size]
                if len(batch) == 1:
                    query = f"SELECT block_hash FROM blocks WHERE block_hash = %(hash)s"
                    result = self.client.execute(query, {'hash': batch[0]})
                else:
                    hash_list = ','.join([f"unhex('{hash_bytes.hex()}')" for hash_bytes in batch])
                    query = f"SELECT block_hash FROM blocks WHERE block_hash IN ({hash_list})"
                    result = self.client.execute(query)
                stored_hashes.extend([row[0] for row in result])
            
            return stored_hashes
            
        except Exception as e:
            self.logger.error(format_error_with_location(e, f"Error in daemon_find_stored_blocks: "))
            return []
    
    def daemon_set_block_height(self, block):
        
        prev_block_hash = self._hex2hash32(block.header.previous_block_hash)
        if prev_block_hash == b'\x00' * 32:
            block.height = 0
            return
        
        query = f"SELECT n_block FROM blocks WHERE block_hash = unhex('{prev_block_hash.hex()}') LIMIT 1"
        result = self.client.execute(query)
        
        assert len(result) > 0, f"Block {block.hash} not found in blocks table"
        assert len(result) == 1, f"Multiple blocks found for hash {block.hash}"
        block.height = result[0][0] + 1
    
    def daemon_load_new_blocks_from_file(self, blockfile_path, stored_hashes, xor_key=None):
        try:
            stored_hashes_set = set(stored_hashes)
            
            for block_raw in get_blocks(blockfile_path, xor_key):
                block = Block(block_raw, None)
                block_hash = self._hex2hash32(block.hash)
                
                if block_hash not in stored_hashes_set:
                    self.daemon_set_block_height(block)
                    inputs_data, outputs_data, blocks_data = self.parse_block(block)
                    
                    if inputs_data is not None:
                        self.insert_data(inputs_data, outputs_data, [blocks_data])
            
            return True
            
        except Exception as e:
            self.logger.error(format_error_with_location(e, f"Error in daemon_load_new_blocks_from_file: "))
            raise e

