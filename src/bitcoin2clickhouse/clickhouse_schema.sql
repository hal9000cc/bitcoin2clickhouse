CREATE TABLE blocks (
    block_hash FixedString(32),
    n_block UInt32 CODEC(Delta, LZ4),
    block_timestamp DateTime,
    version UInt32,
    prev_block_hash FixedString(32),
    merkle_root FixedString(32),
    nonce UInt32,
    bits UInt32,
    size UInt32,
    weight UInt32,
    transaction_count UInt32,
    processed_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (n_block)
SETTINGS index_granularity = 8192;

CREATE TABLE tran_in (
    n_block UInt32 CODEC(Delta, LZ4),
    tx_id FixedString(32),
    input_index UInt16,
    prev_tx_hash FixedString(32),
    prev_tx_index UInt32,
    sequence_number UInt32,
    script_hex String,
    script_type LowCardinality(String),
    is_segwit UInt8,
    witness_count UInt32,
    witness_data Array(String),
    input_size UInt16,
    is_coinbase UInt8,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (n_block, tx_id, input_index)
SETTINGS index_granularity = 8192;

CREATE TABLE tran_out (
    n_block UInt32 CODEC(Delta, LZ4),
    tx_id FixedString(32),
    output_index UInt32,
    value UInt64 CODEC(Delta, LZ4),
    is_coinbase UInt8,
    script_hex String,
    script_type LowCardinality(String),
    is_p2pkh UInt8,
    is_p2sh UInt8,
    is_p2wpkh UInt8,
    is_p2wsh UInt8,
    is_p2tr UInt8,
    is_multisig UInt8,
    is_unknown UInt8,
    is_op_return UInt8,
    address_count UInt16,
    addresses Array(String),
    address_types Array(LowCardinality(String)),
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (n_block, tx_id, output_index)
SETTINGS index_granularity = 8192;

CREATE TABLE db_version (
    version UInt32
) ENGINE = ReplacingMergeTree()
ORDER BY version
SETTINGS index_granularity = 8192;

CREATE VIEW blocks_h AS
SELECT 
    hex(block_hash) as block_hash_h,
    n_block,
    block_timestamp,
    version,
    hex(prev_block_hash) as prev_block_hash_h,
    hex(merkle_root) as merkle_root_h,
    nonce,
    bits,
    size,
    weight,
    transaction_count,
    processed_at
FROM blocks;

CREATE VIEW tran_in_h AS
SELECT 
    n_block,
    hex(tx_id) as tx_id_h,
    input_index,
    hex(prev_tx_hash) as prev_tx_hash_h,
    prev_tx_index,
    sequence_number,
    script_hex,
    script_type,
    is_segwit,
    witness_count,
    witness_data,
    input_size,
    is_coinbase
FROM tran_in;

CREATE VIEW tran_out_h AS
SELECT 
    n_block,
    hex(tx_id) as tx_id_h,
    output_index,
    value,
    script_hex,
    script_type,
    is_p2pkh,
    is_p2sh,
    is_p2wpkh,
    is_p2wsh,
    is_p2tr,
    is_multisig,
    is_unknown,
    is_op_return,
    address_count,
    addresses,
    address_types,
    created_at
FROM tran_out;

CREATE TABLE tx_block
(
    tx_id FixedString(32),
    n_block UInt32
)
ENGINE = ReplacingMergeTree()
ORDER BY tx_id
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW tx_block_mv TO tx_block
AS
SELECT DISTINCT
    tx_id,
    n_block
FROM tran_out;

CREATE TABLE turnover
(
    time DateTime,
    tx_id FixedString(32),
    address String,
    value Decimal64(8),
    is_coinbase UInt8,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (time, tx_id, address)
SETTINGS index_granularity = 8192;

CREATE TABLE turnover_m
(
    time_month Date,
    address String,
    value Decimal64(8),
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(time_month)
ORDER BY (time_month, address)
SETTINGS index_granularity = 8192;

CREATE TABLE turnover_change_point
(
    n_block UInt32,
    version DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
ORDER BY tuple()
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW turnover_change_point_mv TO turnover_change_point
AS
SELECT 
    min_block AS n_block,
    now() AS version
FROM (
    SELECT (SELECT min(n_block) FROM blocks) AS min_block
)
WHERE min_block < COALESCE((SELECT argMax(n_block, version) FROM turnover_change_point), 999999999);

CREATE TABLE turnover_m_change_point
(
    n_block UInt32,
    version DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
ORDER BY tuple()
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW turnover_m_change_point_mv TO turnover_m_change_point
AS
SELECT 
    min_block AS n_block,
    now() AS version
FROM (
    SELECT (SELECT min(n_block) FROM blocks) AS min_block
)
WHERE min_block < COALESCE((SELECT argMax(n_block, version) FROM turnover_m_change_point), 999999999);

INSERT INTO db_version VALUES (1);

