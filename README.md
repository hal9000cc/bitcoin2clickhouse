# Bitcoin2ClickHouse

Loading data from bitcoin-core node blockchain into ClickHouse database. Loads Bitcoin blocks and saves them in processed form for subsequent analysis. Information is saved as:
- transaction inputs/outputs
- address turnovers by transactions
- monthly aggregated address turnovers

## Features

- **Batch multi-threaded loading for initial setup**
- **Daemon for realtime loading**
- **Data integrity check utility**

## System Requirements
### Operating System
I use Linux Mint. It should work well on any Linux system where bitcoin-core runs. Automatic daemon installation should work correctly on Mint, Ubuntu and other distributions from this family. If not - the daemon can be installed manually.

### CPU
I use AMD Ryzen Threadripper with 32 cores. Parsing the entire blockchain in 32 threads takes less than a day. You can use a weaker processor, but parsing time will be longer.

After parsing the blockchain, you also need to generate turnovers (tables turnover and turnover_m), their duration was also measured in hours. I didn't record the exact time, but it's less than block parsing.

### Memory
I use 256 GB RAM. ClickHouse queries in Bitcoin2ClickHouse are designed for this size. With less RAM, some queries will likely fail. If desired, they can be optimized for a smaller memory. I think 128 GB should be fine. With 64 GB - maybe, if you really want to :)

### Disk
The bitcoin-core node with blockchain currently takes 752 GB. The ClickHouse database with loaded blockchain is 1.3 TB. So the minimum required is about 2 TB, but the blockchain is continuously growing.

For reference:
- loaded transactions 926 GB
- transaction turnovers 252 GB
- monthly address turnovers 68 GB

## Installation

### Software Requirements

- Python 3.12+
- ClickHouse Server
- Bitcoin Core
- python-bitcoin-blockchain-parser, for node version with xor encryption you need a fork with support for this (https://github.com/hal9000cc/python-bitcoin-blockchain-parser)

### Installing Dependencies

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install package in development mode
pip install -e .
```

### Configuration

Copy `env.example` to `.env` and configure parameters:

```bash
cp env.example .env
```

Edit the `.env` file:

```env
# Path to Bitcoin blocks directory
BITCOIN_BLOCKS_DIR=/home/user/.bitcoin/blocks

# Path to XOR file (optional, for new node version with xor encryption)
XOR_DAT_PATH=/home/user/.bitcoin/blocks/xor.dat

# ClickHouse connection parameters
CLICKHOUSE_DATABASE=bitcoin
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=bitcoin
CLICKHOUSE_PASSWORD=your_password

# Number of processes for batch loading (recommended: number of CPU cores)
NUM_WORKERS=4

# Batch size for turnover updates
TURNOVER_UPDATE_BATCH_SIZE=10000
```

## Usage

### Daemon for realtime Loading

The daemon automatically monitors new blocks and loads them into ClickHouse:

```bash
# Run daemon in terminal
bitcoin2clickhouse-daemon.py

# Install as systemd service
sudo bitcoin2clickhouse-daemon.py --install

# Service management
sudo systemctl start bitcoin2clickhouse
sudo systemctl stop bitcoin2clickhouse
sudo systemctl status bitcoin2clickhouse
```

The daemon:
- Periodically checks for new blocks
- Automatically loads new blocks
- Updates turnover (turnover tables)

### Batch Block Loading

For initial loading of a large number of blocks, use the batch loading script:

```bash
bitcoin2clickhouse-bulk_load.py
```

It has no parameters, loads everything until finished. Important: When batch downloading, bitcoin-core must be stopped.
Once the batch download is complete, you can start the node and service for real-time downloads - they do not interfere with each other.

### Data Integrity Check

```bash
# Usage help
check_database.py -h

# Run all checks
check_database.py

# Run specific checks
python -m bitcoin2clickhouse.check_database -c 1 2 3

# With batch size and block range parameters
python -m bitcoin2clickhouse.check_database --check 8 --batch-size 5000 --start-block 100000 --end-block 200000
```

Available checks:
1. Genesis block
2. Block sequence
3. Transaction count consistency
4. n_block consistency
5. Previous block hash consistency
6. Transaction hash consistency
7. Previous transaction hash consistency
8. Turnover completeness

## Data Structure

The project creates the following tables in ClickHouse:

- **blocks**: Block information (hash, number, time, transactions, etc.)
- **tran_in**: Transaction inputs
- **tran_out**: Transaction outputs
- **turnover**: Turnovers by transactions and addresses
- **turnover_m**: Address turnovers by months
- **turnover_change_point**: Service table for tracking changes
- **turnover_m_change_point**: Service table for tracking changes

## Logging

Logs are saved to:
- `/var/log/bitcoin2clickhouse/` (if access rights are available)
- `~/.bitcoin2clickhouse/` (in user's home directory)

The daemon log uses the file `daemon.log`, other scripts use corresponding file names.

Critical errors are also sent to systemd journal (when running as a service).

## Query Examples

#Top 10 wallets by coin amount.
```sql
SELECT 
    address,
    sum(value) AS value,
    count(tx_id)
FROM turnover 
GROUP BY address
HAVING value > 10000
ORDER BY value DESC
LIMIT 10
```
| address | value | count(tx_id) |
|:--------|------:|-------------:|
| 34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo | 248597.58110535 | 5491 |
| bc1ql49ydapnjafl5t2cp9zqpjwe6pdgmxy98859v2 | 140574.82562097 | 478 |
| bc1qgdjqv0av3q56jvd82tkdjpy7gdp9ut8tlqmgrpmv24sq90ecnvqqjwvw97 | 130010.07866864 | 312 |
| 3M219KR5vEneNb47ewrPfWyb5jQ2DjxRP6 | 128710.40826881 | 488 |
| bc1qazcm763858nkj2dj986etajv6wquslv8uxwczt | 94643.48489007 | 157 |
| bc1qjasf9z3h7w3jspkhtgatgpyvvzgpa2wwd2lr0eh5tx44reyn2k7sfc27a4 | 87296.46794985 | 133 |
| bc1qd4ysezhmypwty5dnw7c8nqy5h5nxg0xqsvaefd0qn5kq32vwnwqqgv4rzr | 86200.09870865 | 137 |
| 1FeexV6bAHb8ybZjqQMjJrcCrHGW9sb6uF | 79957.26802990 | 658 |
| bc1q8yj0herd4r4yxszw3nkfvt53433thk0f5qst4g | 78317.03554851 | 51 |
| bc1qa5wkgaew2dkv56kfvj49j0av5nml45x9ek9hz6 | 69370.18417293 | 133 |

The data completely matches blockchain explorers at the current moment. Query execution time on my system was 3 minutes.

#Query balance for a single address.

```sql
SELECT 
    sum(value)
FROM turnover
WHERE address = '34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo'
```
sum(value)     |
---------------+
248597.58110535|
Execution time for this query on my system is around 0.1-0.2 seconds.

But it's better to query balance for a single address differently. Use monthly turnovers up to the current month from the turnover_m table, it's significantly more compact. And add current month turnovers from turnover:
```sql
SELECT 
    sum(sums.value) AS value
FROM (
    SELECT 
        sum(value) AS value
    FROM turnover_m 
    WHERE time_month < '2025-11-01' AND address = '34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo'
    UNION ALL
    SELECT 
        sum(value)
    FROM turnover 
    WHERE time >= '2025-11-01' AND address = '34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo'
) sums
```
sum(value)     |
---------------+
248597.58110535|
I can't measure the execution time for this query anymore, the indicator shows 0.0 sec, it's definitely faster than the previous version.


## Contacts

GitHub hal9000cc (hal@hal9000.cc)
