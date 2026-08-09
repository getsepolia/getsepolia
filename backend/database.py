from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from .config import get_settings


SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    recipient_address TEXT NOT NULL,
    payment_wallet TEXT,
    sepolia_amount INTEGER NOT NULL CHECK (sepolia_amount BETWEEN 1 AND 500),
    price_usdc TEXT NOT NULL,
    status TEXT NOT NULL,
    payment_tx_hash TEXT UNIQUE,
    payout_tx_hash TEXT UNIQUE,
    error_message TEXT,
    payout_raw_tx TEXT,
    payout_nonce INTEGER,
    payout_started_at TEXT,
    payout_broadcast_at TEXT,
    payout_confirmed_at TEXT,
    payout_attempts INTEGER NOT NULL DEFAULT 0,
    payout_last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_orders_payment_wallet_created_at
ON orders(payment_wallet, created_at);

CREATE INDEX IF NOT EXISTS idx_orders_status
ON orders(status);


CREATE TABLE IF NOT EXISTS faucet_claims (
    id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    recipient_address TEXT NOT NULL,
    ip_hash TEXT NOT NULL,
    amount_wei INTEGER NOT NULL CHECK (amount_wei > 0),
    status TEXT NOT NULL,
    payout_tx_hash TEXT UNIQUE,
    error_message TEXT,
    payout_raw_tx TEXT,
    payout_nonce INTEGER,
    payout_started_at TEXT,
    payout_broadcast_at TEXT,
    payout_confirmed_at TEXT,
    payout_attempts INTEGER NOT NULL DEFAULT 0,
    payout_last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_faucet_claims_ip_created_at
ON faucet_claims(ip_hash, created_at);

CREATE INDEX IF NOT EXISTS idx_faucet_claims_recipient_created_at
ON faucet_claims(recipient_address, created_at);

CREATE INDEX IF NOT EXISTS idx_faucet_claims_status
ON faucet_claims(status);
"""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def connect() -> sqlite3.Connection:
    settings = get_settings()
    database_path = Path(settings.database_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(
        database_path,
        timeout=30,
        isolation_level=None,
        check_same_thread=False,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


@contextmanager
def transaction() -> Iterator[sqlite3.Connection]:
    connection = connect()
    try:
        connection.execute("BEGIN IMMEDIATE")
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _migrate_payment_wallet_to_nullable(
    connection: sqlite3.Connection,
) -> None:
    """
    SQLite cannot remove a NOT NULL constraint in place, so an installation
    created by the older schema must be rebuilt once.

    The replacement schema already includes the payout-worker columns, so
    existing payout state is preserved when those columns are present.
    """
    table_info = connection.execute(
        "PRAGMA table_info(orders)"
    ).fetchall()

    if not table_info:
        return

    payment_wallet_column = next(
        (row for row in table_info if row["name"] == "payment_wallet"),
        None,
    )

    if payment_wallet_column is None or not payment_wallet_column["notnull"]:
        return

    existing_columns = {row["name"] for row in table_info}

    target_columns = [
        "id",
        "created_at",
        "expires_at",
        "recipient_address",
        "payment_wallet",
        "sepolia_amount",
        "price_usdc",
        "status",
        "payment_tx_hash",
        "payout_tx_hash",
        "error_message",
        "payout_raw_tx",
        "payout_nonce",
        "payout_started_at",
        "payout_broadcast_at",
        "payout_confirmed_at",
        "payout_attempts",
        "payout_last_error",
    ]

    copy_columns = [
        name for name in target_columns
        if name in existing_columns
    ]

    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute("DROP TABLE IF EXISTS orders_new")
        connection.execute(
            """
            CREATE TABLE orders_new (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                recipient_address TEXT NOT NULL,
                payment_wallet TEXT,
                sepolia_amount INTEGER NOT NULL
                    CHECK (sepolia_amount BETWEEN 1 AND 500),
                price_usdc TEXT NOT NULL,
                status TEXT NOT NULL,
                payment_tx_hash TEXT UNIQUE,
                payout_tx_hash TEXT UNIQUE,
                error_message TEXT,
                payout_raw_tx TEXT,
                payout_nonce INTEGER,
                payout_started_at TEXT,
                payout_broadcast_at TEXT,
                payout_confirmed_at TEXT,
                payout_attempts INTEGER NOT NULL DEFAULT 0,
                payout_last_error TEXT
            )
            """
        )

        columns_sql = ", ".join(copy_columns)
        connection.execute(
            f"""
            INSERT INTO orders_new ({columns_sql})
            SELECT {columns_sql}
            FROM orders
            """
        )

        connection.execute("DROP TABLE orders")
        connection.execute("ALTER TABLE orders_new RENAME TO orders")

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_orders_payment_wallet_created_at
            ON orders(payment_wallet, created_at)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_orders_status
            ON orders(status)
            """
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def initialize_database() -> None:
    connection = connect()
    try:
        connection.executescript(SCHEMA)
        _migrate_payment_wallet_to_nullable(connection)
        connection.executescript(SCHEMA)
    finally:
        connection.close()
