CREATE TABLE blocks (
    block_hash String,
    block_height UInt32,
    block_timestamp DateTime,
    version UInt32,
    prev_block_hash String,
    merkle_root String,
    nonce UInt32,
    bits UInt32,
    size UInt32,
    weight UInt32,
    transaction_count UInt32,
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (block_height, block_hash)
SETTINGS index_granularity = 8192;

CREATE TABLE tran_in (
    block_height UInt32,
    transaction_hash String,
    input_index UInt32,
    prev_tx_hash String,
    prev_tx_index UInt32,
    sequence_number UInt32,
    script_hex String,
    script_length UInt32,
    script_type String,
    is_segwit UInt8,
    witness_count UInt32,
    witness_data Array(String),
    input_size UInt32,
    is_coinbase UInt8,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (block_height, transaction_hash, input_index)
SETTINGS index_granularity = 8192;

CREATE INDEX idx_tran_in_tx_hash ON tran_in (transaction_hash) TYPE bloom_filter GRANULARITY 1;

CREATE TABLE tran_out (
    block_height UInt32,
    transaction_hash String,
    output_index UInt32,
    value UInt64,
    script_hex String,
    script_length UInt32,
    script_type String,
    is_p2pkh UInt8,
    is_p2sh UInt8,
    is_p2wpkh UInt8,
    is_p2wsh UInt8,
    is_p2tr UInt8,
    is_multisig UInt8,
    is_unknown UInt8,
    is_op_return UInt8,
    address_count UInt32,
    addresses Array(String),
    address_types Array(String),
    output_size UInt32,
    is_spent UInt8 DEFAULT 0,
    spent_tx_hash String DEFAULT '',
    spent_block_height UInt32 DEFAULT 0,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (block_height, transaction_hash, output_index)
SETTINGS index_granularity = 8192;

CREATE INDEX idx_tran_out_tx_hash ON tran_out (transaction_hash) TYPE bloom_filter GRANULARITY 1;

CREATE TABLE db_version (
    version UInt32
) ENGINE = MergeTree()
ORDER BY version
SETTINGS index_granularity = 8192;

INSERT INTO db_version VALUES (1);