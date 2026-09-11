CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS wallets (
    id UUID PRIMARY KEY,
    player_id VARCHAR(255) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    balance NUMERIC(15, 2) NOT NULL CHECK (balance >= 0),
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (player_id, currency)
);

CREATE TABLE IF NOT EXISTS wager_transactions (
    id UUID PRIMARY KEY,
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    provider_id VARCHAR(255) NOT NULL,
    external_transaction_id VARCHAR(255) NOT NULL,

    idempotency_key VARCHAR(255) NOT NULL UNIQUE,
    payload_hash VARCHAR(255) NOT NULL,

    player_id VARCHAR(255) NOT NULL,
    round_id VARCHAR(255),
    game_id VARCHAR(255),
    kind VARCHAR(50) NOT NULL, -- OPENING, BET, WIN, LOSS, REFUND, ROLLBACK
    status VARCHAR(50) NOT NULL, -- PENDING, PENDING_REFERENCE, PROCESSED, REJECTED, FAILED
    failure_code VARCHAR(100),

    amount NUMERIC(15, 2) NOT NULL CHECK (amount >= 0),
    currency VARCHAR(3) NOT NULL,
    reference_external_tx_id VARCHAR(255),

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMP WITH TIME ZONE,

    UNIQUE (provider_id, external_transaction_id)
);

CREATE TABLE IF NOT EXISTS wallet_ledger (
    id UUID PRIMARY KEY,
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    transaction_id UUID NOT NULL REFERENCES wager_transactions(id),
    direction VARCHAR(10) NOT NULL, -- CREDIT ou DEBIT

    amount NUMERIC(15, 2) NOT NULL CHECK (amount >= 0),
    balance_before NUMERIC(15, 2) NOT NULL CHECK (balance_before >= 0),
    balance_after NUMERIC(15, 2) NOT NULL CHECK (balance_after >= 0),

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    UNIQUE (wallet_id, transaction_id)
);

CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inbox (
    message_id VARCHAR(255) NOT NULL,
    consumer_name VARCHAR(255) NOT NULL,
    payload_hash VARCHAR(255),
    received_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (consumer_name, message_id)
);

CREATE INDEX IF NOT EXISTS idx_wager_transactions_wallet_id ON wager_transactions(wallet_id);
CREATE INDEX IF NOT EXISTS idx_wager_transactions_status_pending ON wager_transactions(status) WHERE status = 'PENDING_REFERENCE';
CREATE INDEX IF NOT EXISTS idx_wager_transactions_ref ON wager_transactions(provider_id, reference_external_tx_id);
CREATE INDEX IF NOT EXISTS idx_wallet_ledger_wallet_pagination ON wallet_ledger(wallet_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox(published) WHERE published = FALSE;