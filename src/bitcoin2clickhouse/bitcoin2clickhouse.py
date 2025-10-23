import sys
import os
import logging
from datetime import datetime
from clickhouse_driver import Client
from blockchain_parser.blockchain import Blockchain

class BitcoinClickHouseLoader:
    def __init__(self, clickhouse_host='localhost', clickhouse_port=9000, 
                 clickhouse_user='default', clickhouse_password='', database='default'):
        """Initialize ClickHouse connection"""
        self.logger = logging.getLogger(__name__)
        self._setup_logging()
        self.database = database
        self.clickhouse_host = clickhouse_host
        self.clickhouse_port = clickhouse_port
        self.clickhouse_user = clickhouse_user
        self.clickhouse_password = clickhouse_password
        
        self.client = self.database_connect()
    
    def _setup_logging(self):
        if not self.logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            
            file_handler = logging.FileHandler('bitcoin2clickhouse.log')
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
            self.logger.info(f"Database {self.database} created or already exists")
            
            client = Client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                user=self.clickhouse_user,
                password=self.clickhouse_password,
                database=self.database
            )
            self.logger.info(f"Connected to database {self.database}")
            
            self.database_initialize(client)
            
            return client
                
        except Exception as e:
            self.logger.error(f"Error connecting to database: {e}")
            raise
        
    def database_initialize(self, client):
        if self.database_initialized(client):
            self.logger.info("Database already initialized")
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
                # self.logger.debug(f"Executed query: {query[:50]}...")
            except Exception as e:
                self.logger.error(f"Error executing query: {e}")
                self.logger.error(f"Problematic query: {query}")
                raise e
        
        self.logger.info("Database initialization completed")
    
    def database_initialized(self, client):

        try:
            result = client.execute(
                "SELECT count() FROM db_version"
            )
            return result[0][0] > 0
        except Exception as e:
            self.logger.error(f"Error checking database initialization: {e}")
            return False
    
    def parse_block(self, block):

        inputs_data = []
        outputs_data = []
        block_data = None
        
        block_timestamp = block.header.timestamp
        
        for tx_index, transaction in enumerate(block.transactions):
            for input_index, input_tx in enumerate(transaction.inputs):
                input_data = {
                    'block_height': block.height,
                    'transaction_hash': transaction.hash,
                    'input_index': input_index,
                    'prev_tx_hash': input_tx.transaction_hash,
                    'prev_tx_index': input_tx.transaction_index,
                    'sequence_number': input_tx.sequence_number,
                    'script_hex': input_tx.script.hex,
                    'script_length': len(input_tx.script.hex) // 2,
                    'script_type': self._get_script_type(input_tx.script),
                    'is_segwit': 1 if transaction.is_segwit else 0,
                    'witness_count': len(input_tx.witnesses),
                    'witness_data': [w.hex() for w in input_tx.witnesses],
                    'input_size': input_tx.size,
                    'is_coinbase': 1 if transaction.is_coinbase() else 0,
                    'created_at': datetime.now()
                }
                inputs_data.append(input_data)
            
            for output_index, output_tx in enumerate(transaction.outputs):
                addresses = output_tx.addresses
                address_types = [addr.type for addr in addresses] if addresses else []
                
                output_data = {
                    'block_height': block.height,
                    'transaction_hash': transaction.hash,
                    'output_index': output_index,
                    'value': output_tx.value,
                    'script_hex': output_tx.script.hex,
                    'script_length': len(output_tx.script.hex) // 2,
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
                    'output_size': output_tx.size,
                    'is_spent': 0,
                    'spent_tx_hash': '',
                    'spent_block_height': 0,
                    'created_at': datetime.now()
                }
                outputs_data.append(output_data)
        
        block_data = {
            'block_hash': block.hash,
            'block_height': block.height,
            'block_timestamp': block_timestamp,
            'version': block.header.version,
            'prev_block_hash': block.header.previous_block_hash,
            'merkle_root': block.header.merkle_root,
            'nonce': block.header.nonce,
            'bits': block.header.bits,
            'size': block.size,
            'weight': getattr(block, 'weight', 0),
            'transaction_count': len(block.transactions),
            'processed_at': datetime.now()
        }
        
        return inputs_data, outputs_data, block_data
    
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
            self.logger.info(f"Inserted {len(inputs_data)} inputs")
        
        if outputs_data:
            self.client.execute(
                'INSERT INTO tran_out VALUES',
                outputs_data
            )
            self.logger.info(f"Inserted {len(outputs_data)} outputs")
        
        if blocks_data:
            self.client.execute(
                'INSERT INTO blocks VALUES',
                blocks_data
            )
            self.logger.info(f"Inserted {len(blocks_data)} blocks")
    
    def last_stored_block(self):

        try:
            result = self.client.execute(
                'SELECT count() as total_count, MAX(block_height) as last_height FROM blocks'
            )
            if result and result[0][0] == 0:
                return -1
            if result and result[0][1] is not None and result[0][1] > 0:
                return result[0][1]
            return -1
        except Exception as e:
            self.logger.error(f"Error getting last processed block: {e}")
            raise e
    
    def cleanup_incomplete_data(self, last_processed_height):

        try:
            if last_processed_height >= 0:
                self.client.execute(
                    f'DELETE FROM tran_in WHERE block_height > {last_processed_height}'
                )
                self.client.execute(
                    f'DELETE FROM tran_out WHERE block_height > {last_processed_height}'
                )
                self.logger.info(f"Cleaned up records above block {last_processed_height}")
        except Exception as e:
            self.logger.error(f"Error cleaning up data: {e}")
            raise e
    
    def load(self, blockchain_path, xor_dat_path=None, start_height=0, end_height=None, batch_size=500000):
        """Load specified range of blocks from blockchain"""
        
        if xor_dat_path:
            blockchain = Blockchain(blockchain_path, xor_dat_path)
        else:
            blockchain = Blockchain(blockchain_path)

        self.logger.info(f"Loading blocks from {start_height} to {end_height or 'latest'}")

        blocks = blockchain.get_ordered_blocks(
            blockchain_path + '/index',
            start=start_height,
            end=end_height
        )
        
        inputs_batch = []
        outputs_batch = []
        blocks_batch = []
        
        for block in blocks:
            try:
                inputs_data, outputs_data, block_data = self.parse_block(block)
                inputs_batch.extend(inputs_data)
                outputs_batch.extend(outputs_data)
                blocks_batch.append(block_data)
                
                if len(inputs_batch) >= batch_size or len(outputs_batch) >= batch_size:
                    self.insert_data(inputs_batch, outputs_batch, blocks_batch)
                    inputs_batch = []
                    outputs_batch = []
                    blocks_batch = []
                
                self.logger.info(f"Processed block {block.height}: {block.hash}")
                
            except Exception as e:
                self.logger.error(f"Error processing block {block.height}: {e}")
                continue
        
        if inputs_batch or outputs_batch or blocks_batch:
            self.insert_data(inputs_batch, outputs_batch, blocks_batch)
    
    def load_new(self, blockchain_path, xor_dat_path=None, batch_size=500000):
        """Load new blocks from blockchain (determines range and calls load)"""
        
        last_stored = self.last_stored_block()
        if last_stored >= 0:
            actual_start = last_stored + 1
            self.logger.info(f"Last stored block: {last_stored}")
            self.cleanup_incomplete_data(last_stored)
        else:
            actual_start = 0
            
        self.logger.info(f"Loading new blocks from {actual_start}")
        
        self.load(blockchain_path, xor_dat_path, actual_start, None, batch_size)
        


