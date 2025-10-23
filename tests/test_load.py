import os
import pytest
from dotenv import load_dotenv
from bitcoin2clickhouse import BitcoinClickHouseLoader

load_dotenv()

@pytest.fixture
def bitcoin2clickhouse():
    return BitcoinClickHouseLoader(
        clickhouse_host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
        clickhouse_port=int(os.getenv('CLICKHOUSE_PORT', '9000')),
        clickhouse_user=os.getenv('CLICKHOUSE_USER', 'default'),
        clickhouse_password=os.getenv('CLICKHOUSE_PASSWORD', ''),
        database=os.getenv('CLICKHOUSE_DATABASE', 'bitcoin')
    )

def test_load_first_100_blocks(bitcoin2clickhouse):
    blockchain_path = os.getenv('BITCOIN_BLOCKS_DIR', '/home/user/.bitcoin/blocks')
    
    if not os.path.exists(blockchain_path):
        pytest.skip(f"Bitcoin blocks directory not found: {blockchain_path}")
    
    bitcoin2clickhouse.load(
        blockchain_path=blockchain_path,
        xor_dat_path=os.getenv('XOR_DAT_PATH', None),
        start_height=0,
        end_height=100
    )
    
    result = bitcoin2clickhouse.client.execute(
        'SELECT COUNT(*) as block_count FROM blocks WHERE block_height < 100'
    )
    
    assert result[0][0] == 100

def test_load_new_blocks(bitcoin2clickhouse):
    blockchain_path = os.getenv('BITCOIN_BLOCKS_DIR', '/home/user/.bitcoin/blocks')
    
    if not os.path.exists(blockchain_path):
        pytest.skip(f"Bitcoin blocks directory not found: {blockchain_path}")
    
    bitcoin2clickhouse.load_new(
        blockchain_path=blockchain_path,
        xor_dat_path=os.getenv('XOR_DAT_PATH', None)
    )
    
    result = bitcoin2clickhouse.client.execute(
        'SELECT COUNT(*) as block_count FROM blocks'
    )
    
    assert result[0][0] > 0
