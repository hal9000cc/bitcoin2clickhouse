import sys
import os
import logging
import glob
from datetime import datetime, timedelta, date
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
        self.setup_logging()
        self.database = database
        self.clickhouse_host = clickhouse_host
        self.clickhouse_port = clickhouse_port
        self.clickhouse_user = clickhouse_user
        self.clickhouse_password = clickhouse_password
        
        if update_batch_size is None:
            update_batch_size = int(os.getenv('UPDATE_BATCH_SIZE', '10000'))
        self.update_batch_size = update_batch_size
        
        self.client = self.database_connect()
        self.stop_requested = False
    
    def request_stop(self):
        self.stop_requested = True
    
    def setup_logging(self):
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
                    'tx_id': self.hex2hash32(transaction.txid),
                    'input_index': input_index,
                    'prev_tx_hash': self.hex2hash32(input_tx.transaction_hash),
                    'prev_tx_index': input_tx.transaction_index,
                    'sequence_number': input_tx.sequence_number,
                    'script_hex': input_tx.script.hex,
                    'script_type': self.get_script_type(input_tx.script),
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
                    'n_block': block.height,
                    'tx_id': self.hex2hash32(transaction.txid),
                    'output_index': output_index,
                    'value': output_tx.value,
                    'is_coinbase': 1 if transaction.is_coinbase() else 0,
                    'script_hex': output_tx.script.hex,
                    'script_type': self.get_output_script_type(output_tx),
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
            'block_hash': self.hex2hash32(block.hash),
            'n_block': block.height,
            'block_timestamp': block_timestamp,
            'version': block.header.version,
            'prev_block_hash': self.hex2hash32(block.header.previous_block_hash),
            'merkle_root': self.hex2hash32(block.header.merkle_root),
            'nonce': block.header.nonce,
            'bits': block.header.bits,
            'size': block.size,
            'weight': getattr(block, 'weight', 0),
            'transaction_count': len(block.transactions),
            'processed_at': datetime.now()
        }
        
        return inputs_data, outputs_data, block_data
    
    @staticmethod
    def hex2hash32(hex_string):
        """Convert hex string to bytes for FixedString(32)"""
        if not hex_string:
            return b'\x00' * 32
        return bytes.fromhex(hex_string)
    
    @staticmethod
    def to_hex(value):
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
    
    @staticmethod
    def start_of_month(dt):
        """Get start of month as date from datetime or date."""
        if isinstance(dt, datetime):
            return dt.date().replace(day=1)
        elif isinstance(dt, date):
            return dt.replace(day=1)
        else:
            return dt
    
    @staticmethod
    def next_month(dt):
        """Get start of next month as datetime from datetime or date."""
        if isinstance(dt, datetime):
            dt_date = dt.date()
        elif isinstance(dt, date):
            dt_date = dt
        else:
            dt_date = dt
        
        if dt_date.month == 12:
            next_month_date = date(dt_date.year + 1, 1, 1)
        else:
            next_month_date = date(dt_date.year, dt_date.month + 1, 1)
        
        return datetime.combine(next_month_date, datetime.min.time())
    
    @staticmethod
    def prev_month(dt):
        """Get start of previous month as date from datetime or date."""
        if isinstance(dt, datetime):
            dt_date = dt.date()
        elif isinstance(dt, date):
            dt_date = dt
        else:
            dt_date = dt
        
        if dt_date.month == 1:
            prev_month_date = date(dt_date.year - 1, 12, 1)
        else:
            prev_month_date = date(dt_date.year, dt_date.month - 1, 1)
        
        return prev_month_date
    
    
    def get_script_type(self, script):
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
    
    def get_output_script_type(self, output):
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
    
    def insert_data(self, inputs_data, outputs_data, blocks_data, update_stats=True):

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
            if len(blocks_data) > 2:
                self.logger.info(f'Inserted {len(blocks_data)} blocks')
            else:
                block_info = [(block['n_block'], block['block_timestamp'].strftime('%Y-%m-%d %H:%M:%S')) for block in blocks_data]
                self.logger.info(f"Inserted {len(blocks_data)} blocks: {block_info}")

            if update_stats:
                self.update_all()
    
    def change_point(self, table_name):
        """
        Get change point block number from turnover_change_point or turnover_m_change_point table.
        
        Args:
            table_name: 'turnover' or 'turnover_m'
            
        Returns:
            Block number or None if not found
        """
        change_point_table = f'{table_name}_change_point'

        result = self.client.execute(f"SELECT count(), argMax(n_block, version) FROM {change_point_table}")
        if result and result[0] and result[0][0] is not None and result[0][0] > 0:
            return result[0][1]
        
        result = self.client.execute("SELECT min(n_block) FROM blocks")
        if result and result[0] and result[0][0] is not None:
            return result[0][0] - 1
        
        return -1
    
    def block_time(self, n_block):
        """
        Get block_timestamp for a specific block number.
        
        Args:
            n_block: Block number
            
        Returns:
            block_timestamp (datetime) or None if block not found
        """
        result = self.client.execute(
            "SELECT count(), argMax(block_timestamp, processed_at) FROM blocks WHERE n_block = %(block)s",
            {'block': n_block}
        )
        if result and result[0] and result[0][0] is not None and result[0][0] > 0:
            return result[0][1]
        return None
    
    def update_all(self):
        try:
            self.update_turnover()
            self.update_turnover_m()
        except KeyboardInterrupt:
            self.logger.info("Update interrupted by user (Ctrl+C)")
            raise
        except Exception as e:
            self.logger.error(format_error_with_location(e, f"Error in update_all: "))
    
    def update_turnover_query(self):
        return """
                INSERT INTO turnover (time, tx_id, address, value, is_coinbase)
                WITH
                    ablocks AS (
                        SELECT 
                            argMax(block_timestamp, processed_at) AS time,
                            n_block
                        FROM blocks
                        WHERE n_block IN (SELECT n_block FROM temp_blocks)
                        GROUP BY n_block
                    ),
                    ti AS (
                        SELECT 
                            n_block,
                            tx_id,
                            input_index,
                            argMax(prev_tx_hash, created_at) prev_tx_hash,
                            argMax(prev_tx_index, created_at) prev_tx_index,
                            argMax(is_coinbase, created_at) is_coinbase
                        FROM tran_in
                        WHERE n_block IN (SELECT n_block FROM ablocks)
                        GROUP BY n_block, tx_id, input_index
                    ),
                    prev_blocks AS (
                        SELECT 
                            tx_id,
                            n_block
                        FROM tx_block
                        WHERE tx_id IN (SELECT prev_tx_hash FROM ti WHERE NOT is_coinbase)
                    ),
                    prev_to AS (
                        SELECT 
                            n_block,
                            tx_id,
                            output_index,
                            argMax(value, created_at) value,
                            argMax(address_count, created_at) address_count,
                            argMax(addresses, created_at) addresses,
                            argMax(is_op_return, created_at) is_op_return
                        FROM tran_out
                        WHERE (tran_out.n_block, tran_out.tx_id) IN (SELECT n_block, tx_id FROM prev_blocks)
                        GROUP BY n_block, tx_id, output_index
                    ),
                    to AS (
                        SELECT 
                            n_block,
                            tx_id,
                            output_index,
                            multiIf(
                                argMax(address_count, created_at) > 0,
                                argMax(addresses[1], created_at),
                                argMax(is_op_return, created_at) = 1,
                                '',
                                concat('_', hex(argMax(tx_id, created_at)), '_', toString(argMax(output_index, created_at)))
                            ) AS address,
                            argMax(value, created_at) value,
                            argMax(is_coinbase, created_at) is_coinbase
                        FROM tran_out
                        WHERE n_block IN (SELECT n_block FROM ablocks)
                        GROUP BY n_block, tx_id, output_index
                    )
                SELECT 
                    time,
                    tx_id,
                    address,
                    sum(value) AS value,
                    is_coinbase
                FROM (
                    SELECT 
                        b.time AS time,
                        ti.tx_id AS tx_id,
                        multiIf(
                            p.address_count > 0,
                            p.addresses[1],
                            p.is_op_return = 1,
                            '',
                            concat('_', hex(ti.prev_tx_hash), '_', toString(ti.prev_tx_index))
                        ) AS address,
                        cast(-p.value as Decimal128(8)) / 100000000 AS value,
                        0 AS is_coinbase
                    FROM ti
                    JOIN prev_to p ON ti.prev_tx_hash = p.tx_id AND ti.prev_tx_index = p.output_index
                    JOIN ablocks b ON ti.n_block = b.n_block
                    UNION ALL
                    SELECT 
                        b.time AS time,
                        to.tx_id AS tx_id,
                        to.address AS address,
                        cast(to.value as Decimal128(8)) / 100000000 AS value,
                        to.is_coinbase AS is_coinbase
                    FROM to
                    JOIN ablocks b ON to.n_block = b.n_block
                )
                GROUP BY time, tx_id, address, is_coinbase
                HAVING value != 0
                """
    
    def clear_turnover(self, last_actual_block):
        """
        Clear turnover data starting from the specified block.
        
        Args:
            last_actual_block: Block number to start clearing from (if < 0, truncates entire table)
        """
        if last_actual_block < 0:
            self.client.execute("TRUNCATE TABLE turnover")
            return
        
        last_actual_block_date = self.block_time(last_actual_block)
        
        result = self.client.execute(
            "SELECT count() FROM turnover WHERE time > %(start_date)s",
            {'start_date': last_actual_block_date}
        )
        if result and result[0] and result[0][0] > 0:
            self.client.execute(
                "ALTER TABLE turnover DELETE WHERE time > %(start_date)s SETTINGS mutations_sync = 2",
                {'start_date': last_actual_block_date}
            )
    
    def update_turnover(self):
        try:
            last_actual_block = self.change_point('turnover')
            
            self.clear_turnover(last_actual_block)
            
            start_processing_block = last_actual_block + 1
            
            result = self.client.execute(
                "SELECT count(DISTINCT n_block), min(n_block), max(n_block) FROM blocks WHERE n_block >= %(start_block)s",
                {'start_block': start_processing_block}
            )
            if not result or result[0][0] == 0:
                return
            
            total_count, min_block, max_block = result[0]
            
            if total_count == 0 or min_block is None or max_block is None:
                return
            
            self.logger.info(f"Updating turnover for blocks {min_block} to {max_block} ({total_count} blocks)")
            
            current_block = start_processing_block
            processed_count = 0
            
            while current_block <= max_block:
                if self.stop_requested:
                    self.logger.info("Stop requested, exiting update_turnover loop")
                    break
                
                end_block = min(current_block + self.update_batch_size - 1, max_block)
                
                result = self.client.execute(
                    "SELECT DISTINCT n_block FROM blocks WHERE n_block >= %(start)s AND n_block <= %(end)s ORDER BY n_block",
                    {'start': current_block, 'end': end_block}
                )
                if not result or len(result) == 0:
                    break
                
                block_numbers = [row[0] for row in result]
                
                try:
                    self.client.execute('DROP TABLE IF EXISTS temp_blocks')
                except:
                    pass
                self.client.execute('CREATE TEMPORARY TABLE temp_blocks (n_block UInt32) ENGINE = Memory')
                self.client.execute('INSERT INTO temp_blocks VALUES', [(nb,) for nb in block_numbers])
                
                self.client.execute(self.update_turnover_query())
                
                max_block_in_batch = max(block_numbers)
                self.client.execute(
                    "INSERT INTO turnover_change_point (n_block, version) VALUES",
                    [(max_block_in_batch, datetime.now())]
                )
                
                processed_count += len(block_numbers)
                remaining_count = total_count - processed_count
                self.logger.info(f"Updated turnover for blocks {min(block_numbers)}-{max_block_in_batch} ({len(block_numbers)} blocks, {remaining_count} remaining)")
                
                current_block = max_block_in_batch + 1
        except Exception as e:
            self.logger.error(format_error_with_location(e, f"Error in update_turnover: "))
    
    def update_turnover_m_query(self):
        return """
                    INSERT INTO turnover_m (time_month, address, value)
                    SELECT 
                        toStartOfMonth(time) AS time_month,
                        address,
                        sum(value) AS value
                    FROM (
                        SELECT 
                            time,
                            address,
                            argMax(value, updated_at) AS value
                        FROM turnover t
                        WHERE 
                            t.time >= %(time_start)s
                            AND t.time < %(time_end)s
                        GROUP BY time, tx_id, address
                    )
                    GROUP BY time_month, address
                """
    
    def clear_turnover_m(self, start_block):
        """
        Clear turnover_m data starting from the specified block.
        
        Args:
            start_block: Block number to start clearing from
        """
        if start_block < 0:
            self.client.execute("TRUNCATE TABLE turnover_m")
            return
        
        start_block_date = self.block_time(start_block)
        start_month = self.start_of_month(start_block_date)
        
        result = self.client.execute(
            "SELECT DISTINCT toYYYYMM(time_month) AS partition_id FROM turnover_m WHERE time_month >= %(start_month)s",
            {'start_month': start_month}
        )
        if result:
            for row in result:
                partition_id = str(row[0])
                self.client.execute(f"ALTER TABLE turnover_m DROP PARTITION '{partition_id}'")
    
    def update_turnover_m(self):
        try:
            start_block = self.change_point('turnover_m')
            
            self.clear_turnover_m(start_block)
            
            if start_block < 0:
                start_month = date(2000, 1, 1)
            else:
                start_block_date = self.block_time(start_block)
                start_month = self.start_of_month(start_block_date)
            
            result = self.client.execute(
                "SELECT max(block_timestamp) FROM (SELECT argMax(block_timestamp, processed_at) AS block_timestamp FROM blocks GROUP BY n_block)"
            )
            if not result or not result[0] or result[0][0] is None:
                return
            
            max_block_date = result[0][0]
            max_block_month = self.start_of_month(max_block_date)
            last_completed_month = self.prev_month(max_block_month)
            
            if start_month > last_completed_month:
                return
            
            result = self.client.execute(
                "SELECT DISTINCT toStartOfMonth(time) AS time_month FROM turnover WHERE toStartOfMonth(time) >= %(start_month)s AND toStartOfMonth(time) <= %(last_month)s ORDER BY time_month",
                {'start_month': start_month, 'last_month': last_completed_month}
            )
            
            if not result:
                return
            
            months_to_process = [row[0] for row in result]
            
            for time_month in months_to_process:
                if self.stop_requested:
                    self.logger.info("Stop requested, exiting update_turnover_m loop")
                    break
                
                time_start = datetime.combine(time_month, datetime.min.time())
                time_end = time_start + timedelta(days=32)
                time_end = time_end.replace(day=1)
                
                self.client.execute(self.update_turnover_m_query(), {
                    'time_start': time_start,
                    'time_end': time_end
                })
                
                self.client.execute(
                    "INSERT INTO turnover_m_change_point (n_block, version) "
                    "SELECT max(n_block), now() FROM blocks WHERE block_timestamp < %(time_end)s",
                    {'time_end': time_end}
                )
                
                month_str = time_month.strftime('%Y-%m')
                self.logger.info(f"Updated turnover_m for month {month_str}")
        except Exception as e:
            self.logger.error(format_error_with_location(e, f"Error in update_turnover_m: "))
    
    def last_stored_block(self):

        try:
            result = self.client.execute(
                'SELECT count() as total_count, max(n_block) as last_height FROM blocks'
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
                        SELECT max(n_block)
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

    def check_database_integrity(self, check_numbers=None, batch_size=None, start_block=None, end_block=None):
        """
        Check database integrity. Returns list of errors.
        
        Args:
            check_numbers: List of check numbers to run (1-8). If None, all checks will run.
            batch_size: Batch size for checks that process blocks in batches. Ignored for checks that do not use batches.
            start_block: Start block number for checks that process blocks. Ignored for checks that do not use blocks.
            end_block: End block number for checks that process blocks. Ignored for checks that do not use blocks.
        """
        errors = []
        
        # Map check numbers to their methods
        check_methods = {
            1: self.check_genesis_block,
            2: self.check_block_sequence,
            3: self.check_transaction_count_consistency,
            4: self.check_n_block_consistency,
            5: self.check_prev_block_hash_consistency,
            6: self.check_transaction_hash_consistency,
            7: self.check_prev_tx_hash_consistency,
            8: self.check_turnover_completeness,
        }
        
        # If no specific checks requested, run all
        if check_numbers is None:
            check_numbers = list(check_methods.keys())
        
        # Validate check numbers
        invalid_checks = [c for c in check_numbers if c not in check_methods]
        if invalid_checks:
            error = f"Invalid check numbers: {invalid_checks}. Valid range is 1-8."
            errors.append(error)
            self.logger.error(error)
            return errors
        
        self.logger.info("Starting database integrity check...")
        
        try:
            # First group of checks (1-4)
            first_group = [c for c in check_numbers if c in [1, 2, 3, 4]]
            if first_group:
                for check_num in sorted(first_group):
                    errors.extend(check_methods[check_num](check_number=check_num))
                
                if errors:
                    self.logger.error(f"Check 1-4 completed. Next steps skipped. Found {len(errors)} errors.")
                    return errors
            
            # Second group of checks (5-8) - only run if first group passed or wasn't requested
            second_group = [c for c in check_numbers if c in [5, 6, 7, 8]]
            if second_group:
                for check_num in sorted(second_group):
                    if check_num == 8:
                        # Pass batch_size, start_block, end_block only to check_turnover_completeness
                        errors.extend(check_methods[check_num](
                            check_number=check_num,
                            batch_size=batch_size,
                            start_block=start_block,
                            end_block=end_block
                        ))
                    else:
                        errors.extend(check_methods[check_num](check_number=check_num))
            
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

    def check_genesis_block(self, check_number: int):
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

    def check_n_block_consistency(self, check_number: int):
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

    def check_prev_block_hash_consistency(self, check_number: int):
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

    def check_transaction_hash_consistency(self, check_number: int):
        """transaction_hash consistency between tran_in and tran_out. Returns list of errors."""
        errors = []
        block_batch_size = 100000
        max_display_errors = 100
        
        self.logger.info(f"Check {check_number}: transaction_hash consistency between tran_in and tran_out...")
        
        try:
            max_block_result = self.client.execute("SELECT max(n_block) FROM tran_in")
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

    def check_prev_tx_hash_consistency(self, check_number: int):
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
                    f"{self.to_hex(row[0])} (in transaction {self.to_hex(row[1])}, input {row[2]})"
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
    
    def check_transaction_count_consistency(self, check_number: int):
        """transaction_count matches actual transaction count in tran_in/tran_out. Returns list of errors."""
        errors = []
        
        self.logger.info(f"Check {check_number}: transaction_count consistency...")
        
        try:
            result = self.client.execute("""
                SELECT b.n_block, min(b.transaction_count), count(DISTINCT ti.tx_id) as actual_count
                FROM blocks b
                LEFT JOIN tran_in ti ON b.n_block = ti.n_block
                GROUP BY b.n_block
                HAVING min(b.transaction_count) != actual_count
            """)
            
            inconsistent_counts_in = [f"Block {row[0]}: declared {row[1]} transactions, actual in tran_in {row[2]}" for row in result]
            
            if inconsistent_counts_in:
                error = f"Error: transaction_count inconsistency in tran_in: {inconsistent_counts_in}"
                errors.append(error)
                self.logger.error(error)
            
            result = self.client.execute("""
                SELECT b.n_block, min(b.transaction_count), count(DISTINCT to.tx_id) as actual_count
                FROM blocks b
                LEFT JOIN tran_out to ON b.n_block = to.n_block
                GROUP BY b.n_block
                HAVING min(b.transaction_count) != actual_count
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
    
    def check_block_sequence(self, check_number: int):
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
    
    def check_turnover_completeness(self, check_number: int, batch_size=None, start_block=None, end_block=None):
        """
        Check that all transactions from blocks are present in turnover table. Returns list of errors.
        
        Args:
            check_number: Check number
            batch_size: Batch size for processing blocks (default: 10000). Ignored if None.
            start_block: Start block number. If None, uses min(n_block) from blocks.
            end_block: End block number. If None, uses max(n_block) from blocks.
        """
        errors = []
        
        self.logger.info(f"Check {check_number}: completeness of turnover...")
        
        # Default batch size
        if batch_size is None:
            batch_size = 10000
        
        try:
            # Get block range from database if not fully specified
            result = self.client.execute("""
                SELECT min(n_block), max(n_block)
                FROM blocks
            """)
            
            if not result or not result[0] or result[0][0] is None:
                self.logger.info("✓ No blocks to check turnover completeness")
                return errors
            
            db_min_block, db_max_block = result[0]
            
            if db_min_block is None or db_max_block is None:
                self.logger.info("✓ No blocks to check turnover completeness")
                return errors
            
            # Use provided values or database values
            min_block = start_block if start_block is not None else db_min_block
            max_block = end_block if end_block is not None else db_max_block
            
            if max_block > db_max_block:
                max_block = db_max_block
            
            self.logger.info(f"Checking turnover completeness for blocks {min_block} to {max_block} (batch size: {batch_size})")
            
            missing_blocks = []
            current_block = min_block
            
            while current_block <= max_block:
                batch_end_block = min(current_block + batch_size - 1, max_block)
                
                result = self.client.execute("""
                    WITH
                        ablocks AS (
                            SELECT 
                                argMax(block_timestamp, processed_at) AS time,
                                n_block
                            FROM blocks
                            WHERE n_block >= %(start_block)s AND n_block <= %(end_block)s
                            GROUP BY n_block
                        ),
                        ti AS (
                            SELECT 
                                n_block,
                                tx_id,
                                input_index,
                                argMax(prev_tx_hash, created_at) prev_tx_hash,
                                argMax(prev_tx_index, created_at) prev_tx_index,
                                argMax(is_coinbase, created_at) is_coinbase
                            FROM tran_in
                            WHERE n_block IN (SELECT n_block FROM ablocks)
                            GROUP BY n_block, tx_id, input_index
                        ),
                        prev_blocks AS (
                            SELECT 
                                tx_id,
                                n_block
                            FROM tx_block
                            WHERE tx_id IN (SELECT prev_tx_hash FROM ti WHERE NOT is_coinbase)
                        ),
                        prev_to AS (
                            SELECT 
                                n_block,
                                tx_id,
                                output_index,
                                argMax(value, created_at) value,
                                argMax(address_count, created_at) address_count,
                                argMax(addresses, created_at) addresses,
                                argMax(is_op_return, created_at) is_op_return
                            FROM tran_out
                            WHERE (tran_out.n_block, tran_out.tx_id) IN (SELECT n_block, tx_id FROM prev_blocks)
                            GROUP BY n_block, tx_id, output_index
                        ),
                        to AS (
                            SELECT 
                                n_block,
                                tx_id,
                                output_index,
                                multiIf(
                                    argMax(address_count, created_at) > 0,
                                    argMax(addresses[1], created_at),
                                    argMax(is_op_return, created_at) = 1,
                                    '',
                                    concat('_', hex(argMax(tx_id, created_at)), '_', toString(argMax(output_index, created_at)))
                                ) AS address,
                                argMax(value, created_at) value,
                                argMax(is_coinbase, created_at) is_coinbase
                            FROM tran_out
                            WHERE n_block IN (SELECT n_block FROM ablocks)
                            GROUP BY n_block, tx_id, output_index
                        ),
                        tv AS (
                            SELECT DISTINCT time, tx_id 
                            FROM turnover 
                            WHERE time IN (SELECT time FROM ablocks)
                        ),
                        dtx AS (
                            SELECT 
                                time,
                                tx_id tx_id,
                                sum(value) AS value
                            FROM (
                                SELECT 
                                    b.time AS time,
                                    ti.tx_id AS tx_id,
                                    cast(-p.value as Decimal128(8)) / 100000000 AS value
                                FROM ti
                                JOIN prev_to p ON ti.prev_tx_hash = p.tx_id AND ti.prev_tx_index = p.output_index
                                JOIN ablocks b ON ti.n_block = b.n_block
                                UNION ALL
                                SELECT 
                                    b.time AS time,
                                    to.tx_id AS tx_id,
                                    cast(to.value as Decimal128(8)) / 100000000 AS value
                                FROM to
                                JOIN ablocks b ON to.n_block = b.n_block
                            )
                            GROUP BY time, tx_id
                            HAVING value != 0
                        ),
                        wrong_tx AS (
                            SELECT DISTINCT dtx.tx_id
                            FROM dtx 
                            ANTI JOIN tv ON tv.time = dtx.time AND tv.tx_id = dtx.tx_id
                        )
                    SELECT 
                        to.n_block,
                        count(*)
                    FROM tran_out to
                    WHERE to.tx_id IN (SELECT tx_id FROM wrong_tx)
                    GROUP BY to.n_block
                """, {
                    'start_block': current_block,
                    'end_block': batch_end_block
                })
                
                if result:
                    batch_missing = [(row[0], row[1]) for row in result]  # (n_block, count)
                    missing_blocks.extend(batch_missing)
                    total_missing_tx = sum(count for _, count in batch_missing)
                    self.logger.warning(f"Found {len(batch_missing)} blocks with {total_missing_tx} missing transactions in turnover for blocks {current_block}-{batch_end_block}")
                
                self.logger.info(f"Checked blocks {current_block}-{batch_end_block} ({batch_end_block - current_block + 1} blocks)")
                
                current_block = batch_end_block + 1
            
            if missing_blocks:
                total_missing_tx = sum(count for _, count in missing_blocks)
                # Limit error message to first 50 blocks
                display_missing = missing_blocks[:50]
                display_text = [f"Block {n_block}: {count} transactions" for n_block, count in display_missing]
                error = f"Error: {len(missing_blocks)} blocks with {total_missing_tx} missing transactions in turnover table. First 50 blocks: {display_text}"
                errors.append(error)
                self.logger.error(error)
            else:
                self.logger.info("✓ Turnover completeness is correct")
        
        except Exception as e:
            error = f"Error in check {check_number} (turnover completeness): {e}"
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
                            worker_loader.insert_data(inputs_batch, outputs_batch, blocks_batch, update_stats=False)
                            inputs_batch = []
                            outputs_batch = []
                            blocks_batch = []
                except Exception as e:
                    logger.error(format_error_with_location(e, f"Worker {worker_id}: Error processing block {block_idx.height}: "))
                    raise e
            
            if inputs_batch or outputs_batch or blocks_batch:
                worker_loader.insert_data(inputs_batch, outputs_batch, blocks_batch, update_stats=False)
            
            return f"Worker {worker_id}: Successfully loaded {len(block_indexes)} blocks"
            
        except Exception as e:
            return f"Worker {worker_id}: Failed with error: {e}"
    
    def daemon_find_stored_blocks(self, blockfile_path, xor_key=None):
        try:
            block_hashes = []
            for block_raw in get_blocks(blockfile_path, xor_key):
                block = Block(block_raw, None)
                block_hash = self.hex2hash32(block.hash)
                block_hashes.append(block_hash)
            
            if not block_hashes:
                return []

            stored_hashes = []
            batch_size = 1000
            for i in range(0, len(block_hashes), batch_size):
                batch = block_hashes[i:i + batch_size]
                hash_list = ','.join([f"unhex('{hash_bytes.hex()}')" for hash_bytes in batch])
                query = f"SELECT block_hash FROM blocks FINAL WHERE block_hash IN ({hash_list})"
                result = self.client.execute(query)
                stored_hashes.extend([row[0] for row in result])
            
            return stored_hashes
            
        except Exception as e:
            self.logger.error(format_error_with_location(e, f"Error in daemon_find_stored_blocks: "))
            return []

    def block_height(self, block):
        
        prev_block_hash = self.hex2hash32(block.header.previous_block_hash)
        if prev_block_hash == b'\x00' * 32:
            block.height = 0
            return
        
        query = f"SELECT n_block FROM blocks FINAL WHERE block_hash = unhex('{prev_block_hash.hex()}') LIMIT 1"
        result = self.client.execute(query)
        
        if len(result) == 0:
            return None
            
        assert len(result) == 1, f"Multiple blocks found for hash {block.hash}"

        return result[0][0] + 1
    
    def daemon_load_new_blocks_from_file(self, blockfile_path, stored_hashes, xor_key=None):
        try:
            stored_hashes_set = set(stored_hashes)
            
            while True:

                n_skipped = 0
                for block_raw in get_blocks(blockfile_path, xor_key):
                    block = Block(block_raw, None)
                    block_hash = self.hex2hash32(block.hash)
                    
                    if block_hash not in stored_hashes_set:

                        block_height = self.block_height(block)
                        if block_height is None:
                            n_skipped += 1
                            continue

                        block.height = block_height
                        inputs_data, outputs_data, blocks_data = self.parse_block(block)
                        
                        if inputs_data is not None:
                            self.insert_data(inputs_data, outputs_data, [blocks_data])

                        stored_hashes_set.add(block_hash)

                if n_skipped == 0:
                    break

            return True
            
        except Exception as e:
            self.logger.error(format_error_with_location(e, f"Error in daemon_load_new_blocks_from_file: "))
            raise e

