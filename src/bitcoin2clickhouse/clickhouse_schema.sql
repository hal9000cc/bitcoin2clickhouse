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
ORDER BY (n_block, block_hash)
SETTINGS index_granularity = 8192;

CREATE TABLE tran_in (
    n_block UInt32 CODEC(Delta, LZ4),
    transaction_hash FixedString(32),
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
    is_coinbase UInt8
) ENGINE = ReplacingMergeTree()
ORDER BY (n_block, transaction_hash, input_index)
SETTINGS index_granularity = 8192;

CREATE INDEX idx_tran_in_tx_hash ON tran_in (transaction_hash) TYPE bloom_filter GRANULARITY 1;

CREATE TABLE tran_out (
    n_block UInt32 CODEC(Delta, LZ4),
    transaction_hash FixedString(32),
    output_index UInt32,
    value UInt64 CODEC(Delta, LZ4),
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
ORDER BY (n_block, transaction_hash, output_index)
SETTINGS index_granularity = 8192;

CREATE INDEX idx_tran_out_tx_hash ON tran_out (transaction_hash) TYPE bloom_filter GRANULARITY 1;

CREATE TABLE db_version (
    version UInt32
) ENGINE = ReplacingMergeTree()
ORDER BY version
SETTINGS index_granularity = 8192;

INSERT INTO db_version VALUES (1);