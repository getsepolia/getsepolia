#!/usr/bin/env python3
from __future__ import annotations

import logging
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, TypeVar

from dotenv import load_dotenv
from eth_account import Account
from hexbytes import HexBytes
from web3 import HTTPProvider, Web3
from web3.exceptions import TransactionNotFound

from .database import initialize_database, transaction

SEPOLIA_CHAIN_ID = 11155111
T = TypeVar("T")
PROJECT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_DIR / ".env", override=False)
LOGGER = logging.getLogger("sepolia_payout_worker")


@dataclass(frozen=True)
class WorkerSettings:
    rpc_urls: tuple[str, ...]
    private_key: str
    payout_address: str
    confirmations: int
    poll_interval_seconds: int
    receipt_timeout_seconds: int
    gas_limit: int
    priority_fee_gwei: Decimal
    max_fee_multiplier: Decimal
    balance_reserve_eth: Decimal
    max_attempts: int
    log_level: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Required environment variable {name} is missing.")
    return value


def parse_int_env(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)).strip())
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer.") from exc
    if not minimum <= value <= maximum:
        raise RuntimeError(f"{name} must be between {minimum} and {maximum}.")
    return value


def parse_decimal_env(name: str, default: str, minimum: Decimal) -> Decimal:
    try:
        value = Decimal(os.getenv(name, default).strip())
    except InvalidOperation as exc:
        raise RuntimeError(f"{name} must be a decimal number.") from exc
    if not value.is_finite() or value < minimum:
        raise RuntimeError(f"{name} must be at least {minimum}.")
    return value


def parse_rpc_urls() -> tuple[str, ...]:
    urls: list[str] = []
    primary = os.getenv("SEPOLIA_RPC_URL", "").strip()
    if primary:
        urls.append(primary)
    for item in os.getenv("SEPOLIA_RPC_BACKUP_URLS", "").replace("\n", ",").split(","):
        item = item.strip()
        if item:
            urls.append(item)
    urls = list(dict.fromkeys(urls))
    if not urls:
        raise RuntimeError("Configure SEPOLIA_RPC_URL and/or SEPOLIA_RPC_BACKUP_URLS.")
    return tuple(urls)


def normalize_private_key(value: str) -> str:
    value = value.strip()
    if value.startswith("0x"):
        value = value[2:]
    if len(value) != 64:
        raise RuntimeError("SEPOLIA_PAYOUT_PRIVATE_KEY must contain exactly 32 bytes.")
    try:
        int(value, 16)
    except ValueError as exc:
        raise RuntimeError("SEPOLIA_PAYOUT_PRIVATE_KEY is not hexadecimal.") from exc
    return value


def load_settings() -> WorkerSettings:
    private_key = normalize_private_key(required_env("SEPOLIA_PAYOUT_PRIVATE_KEY"))
    account = Account.from_key(private_key)
    configured_address = os.getenv("SEPOLIA_PAYOUT_ADDRESS", account.address).strip()
    payout_address = Web3.to_checksum_address(configured_address)
    if payout_address.lower() != account.address.lower():
        raise RuntimeError("SEPOLIA_PAYOUT_ADDRESS does not match SEPOLIA_PAYOUT_PRIVATE_KEY.")
    return WorkerSettings(
        rpc_urls=parse_rpc_urls(),
        private_key=private_key,
        payout_address=payout_address,
        confirmations=parse_int_env("SEPOLIA_CONFIRMATIONS", 1, 1, 100),
        poll_interval_seconds=parse_int_env("PAYOUT_POLL_INTERVAL_SECONDS", 5, 1, 3600),
        receipt_timeout_seconds=parse_int_env("PAYOUT_RECEIPT_TIMEOUT_SECONDS", 600, 30, 86400),
        gas_limit=parse_int_env("PAYOUT_GAS_LIMIT", 21000, 21000, 500000),
        priority_fee_gwei=parse_decimal_env("SEPOLIA_PRIORITY_FEE_GWEI", "1.0", Decimal("0")),
        max_fee_multiplier=parse_decimal_env("SEPOLIA_MAX_FEE_MULTIPLIER", "2.0", Decimal("1")),
        balance_reserve_eth=parse_decimal_env("SEPOLIA_GAS_RESERVE_ETH", "0.05", Decimal("0")),
        max_attempts=parse_int_env("PAYOUT_MAX_ATTEMPTS", 5, 1, 100),
        log_level=os.getenv("PAYOUT_LOG_LEVEL", "INFO").strip().upper(),
    )


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level, logging.INFO), format="%(asctime)s %(levelname)s %(name)s: %(message)s")


class RpcPool:
    def __init__(self, urls: tuple[str, ...]) -> None:
        self.providers = [Web3(HTTPProvider(url, request_kwargs={"timeout": 20})) for url in urls]

    def call(self, operation_name: str, callback: Callable[[Web3], T]) -> T:
        errors: list[str] = []
        for index, w3 in enumerate(self.providers, start=1):
            try:
                if not w3.is_connected():
                    raise RuntimeError("connection failed")
                if w3.eth.chain_id != SEPOLIA_CHAIN_ID:
                    raise RuntimeError(f"wrong chain ID {w3.eth.chain_id}")
                return callback(w3)
            except Exception as exc:
                errors.append(f"RPC #{index}: {exc}")
        raise RuntimeError(f"All Sepolia RPCs failed during {operation_name}: " + "; ".join(errors))

    def broadcast(self, raw_transaction: bytes) -> str:
        errors: list[str] = []
        expected_hash = Web3.keccak(raw_transaction).hex()
        for index, w3 in enumerate(self.providers, start=1):
            try:
                if not w3.is_connected() or w3.eth.chain_id != SEPOLIA_CHAIN_ID:
                    raise RuntimeError("connection or chain check failed")
                return w3.eth.send_raw_transaction(raw_transaction).hex()
            except Exception as exc:
                message = str(exc).lower()
                if "already known" in message or "known transaction" in message or "already imported" in message:
                    return expected_hash
                errors.append(f"RPC #{index}: {exc}")
        raise RuntimeError("All Sepolia RPCs rejected the raw transaction: " + "; ".join(errors))


def broadcast(self, raw_transaction: bytes) -> str:
    errors: list[str] = []

    expected_hash = Web3.to_hex(
        Web3.keccak(raw_transaction)
    )

    for index, w3 in enumerate(self.providers, start=1):
        try:
            if not w3.is_connected():
                raise RuntimeError("connection failed")

            if w3.eth.chain_id != SEPOLIA_CHAIN_ID:
                raise RuntimeError(
                    f"wrong chain ID {w3.eth.chain_id}"
                )

            return Web3.to_hex(
                w3.eth.send_raw_transaction(raw_transaction)
            )

        except Exception as exc:
            message = str(exc).lower()

            if (
                "already known" in message
                or "known transaction" in message
                or "already imported" in message
            ):
                return expected_hash

            errors.append(
                f"RPC #{index}: {exc}"
            )

    raise RuntimeError(
        "All Sepolia RPCs rejected the raw transaction: "
        + "; ".join(errors)
    )


def fetch_unresolved_payout() -> sqlite3.Row | None:
    with transaction() as connection:
        return connection.execute(
            """
            SELECT * FROM orders
            WHERE status = 'payout_pending' AND payout_tx_hash IS NOT NULL
            ORDER BY payout_started_at ASC, created_at ASC
            LIMIT 1
            """
        ).fetchone()


def claim_next_order(max_attempts: int) -> sqlite3.Row | None:
    with transaction() as connection:
        candidate = connection.execute(
            """
            SELECT id FROM orders
            WHERE status = 'payment_confirmed'
              AND payout_tx_hash IS NULL
              AND COALESCE(payout_attempts, 0) < ?
            ORDER BY created_at ASC LIMIT 1
            """,
            (max_attempts,),
        ).fetchone()
        if candidate is None:
            return None
        cursor = connection.execute(
            """
            UPDATE orders
            SET status='payout_pending', payout_started_at=?,
                payout_attempts=COALESCE(payout_attempts,0)+1,
                payout_last_error=NULL, error_message=NULL
            WHERE id=? AND status='payment_confirmed' AND payout_tx_hash IS NULL
            """,
            (utc_now_iso(), candidate["id"]),
        )
        if cursor.rowcount != 1:
            return None
        return connection.execute("SELECT * FROM orders WHERE id=?", (candidate["id"],)).fetchone()


def return_claim_to_queue(order_id: str, error: str) -> None:
    with transaction() as connection:
        connection.execute(
            """
            UPDATE orders SET status='payment_confirmed', payout_started_at=NULL,
                payout_last_error=?, error_message=?
            WHERE id=? AND status='payout_pending' AND payout_tx_hash IS NULL
            """,
            (error[:1000], error[:1000], order_id),
        )


def mark_manual_review(order_id: str, error: str) -> None:
    with transaction() as connection:
        connection.execute("UPDATE orders SET status='manual_review', payout_last_error=?, error_message=? WHERE id=?", (error[:1000], error[:1000], order_id))


def store_signed_transaction(order_id: str, tx_hash: str, raw_tx_hex: str, nonce: int) -> None:
    with transaction() as connection:
        cursor = connection.execute(
            """
            UPDATE orders SET payout_tx_hash=?, payout_raw_tx=?, payout_nonce=?, payout_last_error=NULL
            WHERE id=? AND status='payout_pending' AND payout_tx_hash IS NULL
            """,
            (tx_hash.lower(), raw_tx_hex, nonce, order_id),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Could not atomically store the signed payout transaction.")


def mark_broadcast(order_id: str) -> None:
    with transaction() as connection:
        connection.execute("UPDATE orders SET payout_broadcast_at=COALESCE(payout_broadcast_at,?), payout_last_error=NULL WHERE id=?", (utc_now_iso(), order_id))


def store_payout_error(order_id: str, error: str) -> None:
    with transaction() as connection:
        connection.execute("UPDATE orders SET payout_last_error=?, error_message=? WHERE id=?", (error[:1000], error[:1000], order_id))


def mark_delivered(order_id: str) -> None:
    with transaction() as connection:
        cursor = connection.execute(
            """
            UPDATE orders SET status='delivered', payout_confirmed_at=?, payout_last_error=NULL, error_message=NULL
            WHERE id=? AND status='payout_pending'
            """,
            (utc_now_iso(), order_id),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Order could not be marked delivered.")


def mark_failed(order_id: str, error: str) -> None:
    with transaction() as connection:
        connection.execute("UPDATE orders SET status='failed', payout_last_error=?, error_message=? WHERE id=? AND status='payout_pending'", (error[:1000], error[:1000], order_id))


def build_and_store_payout(order: sqlite3.Row, settings: WorkerSettings, rpc: RpcPool) -> sqlite3.Row:
    account = Account.from_key(settings.private_key)
    recipient = Web3.to_checksum_address(order["recipient_address"])
    amount_wei = int(order["sepolia_amount"]) * 10**18
    reserve_wei = int(settings.balance_reserve_eth * Decimal(10**18))

    def build(w3: Web3) -> dict[str, Any]:
        nonce = w3.eth.get_transaction_count(settings.payout_address, "pending")
        latest_block = w3.eth.get_block("latest")
        base_fee = latest_block.get("baseFeePerGas")
        if base_fee is None:
            raise RuntimeError("Sepolia RPC did not return baseFeePerGas.")
        priority_fee = int(settings.priority_fee_gwei * Decimal(10**9))
        max_fee = int(Decimal(base_fee) * settings.max_fee_multiplier) + priority_fee
        required = amount_wei + settings.gas_limit * max_fee + reserve_wei
        balance = w3.eth.get_balance(settings.payout_address)
        if balance < required:
            raise RuntimeError(
                f"Insufficient Sepolia ETH: need at most {Web3.from_wei(required,'ether')} ETH, have {Web3.from_wei(balance,'ether')} ETH."
            )
        return {
            "chainId": SEPOLIA_CHAIN_ID,
            "from": settings.payout_address,
            "to": recipient,
            "value": amount_wei,
            "nonce": nonce,
            "gas": settings.gas_limit,
            "maxFeePerGas": max_fee,
            "maxPriorityFeePerGas": priority_fee,
            "type": 2,
        }

    tx = rpc.call("build payout", build)
    signed = account.sign_transaction(tx)
    raw = bytes(signed.raw_transaction)
    store_signed_transaction(
        order["id"],
        Web3.to_hex(signed.hash),
        Web3.to_hex(raw),
        tx["nonce"],
    )
    with transaction() as connection:
        stored = connection.execute("SELECT * FROM orders WHERE id=?", (order["id"],)).fetchone()
    if stored is None:
        raise RuntimeError("Order disappeared after storing payout.")
    return stored


def reconcile_payout(order: sqlite3.Row, settings: WorkerSettings, rpc: RpcPool) -> bool:
    tx_hash = order["payout_tx_hash"]
    raw_tx_hex = order["payout_raw_tx"]
    if not tx_hash or not raw_tx_hex:
        raise RuntimeError("Payout-pending order has incomplete transaction metadata.")

    def get_receipt(w3: Web3):
        try:
            return w3.eth.get_transaction_receipt(tx_hash)
        except TransactionNotFound:
            return None

    receipt = rpc.call("get payout receipt", get_receipt)
    if receipt is None:
        broadcast_hash = rpc.broadcast(bytes(HexBytes(raw_tx_hex)))
        if Web3.to_hex(HexBytes(broadcast_hash)).lower() != Web3.to_hex(HexBytes(tx_hash)).lower():
            raise RuntimeError(
                "Broadcast transaction hash does not match stored hash."
            )
        mark_broadcast(order["id"])
        if order["payout_started_at"]:
            started = datetime.fromisoformat(order["payout_started_at"])
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            if elapsed > settings.receipt_timeout_seconds:
                error = f"Payout still not mined after {int(elapsed)} seconds. Manual review required. Transaction: {tx_hash}"
                mark_manual_review(order["id"], error)
                LOGGER.error(error)
                return False
        LOGGER.info("Payout %s for order %s is pending.", tx_hash, order["id"])
        return True

    if int(receipt["status"]) != 1:
        error = f"Sepolia payout transaction reverted: {tx_hash}"
        mark_failed(order["id"], error)
        LOGGER.error(error)
        return False

    latest_block = rpc.call("get latest Sepolia block", lambda w3: w3.eth.block_number)
    confirmations = latest_block - receipt["blockNumber"] + 1
    if confirmations < settings.confirmations:
        LOGGER.info("Payout %s has %s/%s confirmations.", tx_hash, confirmations, settings.confirmations)
        return True

    mark_delivered(order["id"])
    LOGGER.info("Order %s delivered in transaction %s.", order["id"], tx_hash)
    return False



def fetch_unresolved_faucet_payout() -> sqlite3.Row | None:
    with transaction() as connection:
        return connection.execute(
            """
            SELECT *
            FROM faucet_claims
            WHERE status = 'payout_pending'
              AND payout_tx_hash IS NOT NULL
            ORDER BY payout_started_at ASC, created_at ASC
            LIMIT 1
            """
        ).fetchone()


def claim_next_faucet(max_attempts: int) -> sqlite3.Row | None:
    with transaction() as connection:
        candidate = connection.execute(
            """
            SELECT id
            FROM faucet_claims
            WHERE status = 'queued'
              AND payout_tx_hash IS NULL
              AND COALESCE(payout_attempts, 0) < ?
            ORDER BY created_at ASC
            LIMIT 1
            """,
            (max_attempts,),
        ).fetchone()

        if candidate is None:
            return None

        cursor = connection.execute(
            """
            UPDATE faucet_claims
            SET status = 'payout_pending',
                payout_started_at = ?,
                payout_attempts = COALESCE(payout_attempts, 0) + 1,
                payout_last_error = NULL,
                error_message = NULL
            WHERE id = ?
              AND status = 'queued'
              AND payout_tx_hash IS NULL
            """,
            (utc_now_iso(), candidate["id"]),
        )

        if cursor.rowcount != 1:
            return None

        return connection.execute(
            "SELECT * FROM faucet_claims WHERE id = ?",
            (candidate["id"],),
        ).fetchone()


def return_faucet_to_queue(claim_id: str, error: str) -> None:
    with transaction() as connection:
        connection.execute(
            """
            UPDATE faucet_claims
            SET status = 'queued',
                payout_started_at = NULL,
                payout_last_error = ?,
                error_message = ?
            WHERE id = ?
              AND status = 'payout_pending'
              AND payout_tx_hash IS NULL
            """,
            (error[:1000], error[:1000], claim_id),
        )


def mark_faucet_manual_review(claim_id: str, error: str) -> None:
    with transaction() as connection:
        connection.execute(
            """
            UPDATE faucet_claims
            SET status = 'manual_review',
                payout_last_error = ?,
                error_message = ?
            WHERE id = ?
            """,
            (error[:1000], error[:1000], claim_id),
        )


def store_faucet_signed_transaction(
    claim_id: str,
    tx_hash: str,
    raw_tx_hex: str,
    nonce: int,
) -> None:
    with transaction() as connection:
        cursor = connection.execute(
            """
            UPDATE faucet_claims
            SET payout_tx_hash = ?,
                payout_raw_tx = ?,
                payout_nonce = ?,
                payout_last_error = NULL
            WHERE id = ?
              AND status = 'payout_pending'
              AND payout_tx_hash IS NULL
            """,
            (tx_hash.lower(), raw_tx_hex, nonce, claim_id),
        )
        if cursor.rowcount != 1:
            raise RuntimeError(
                "Could not atomically store the signed faucet transaction."
            )


def mark_faucet_broadcast(claim_id: str) -> None:
    with transaction() as connection:
        connection.execute(
            """
            UPDATE faucet_claims
            SET payout_broadcast_at = COALESCE(payout_broadcast_at, ?),
                payout_last_error = NULL
            WHERE id = ?
            """,
            (utc_now_iso(), claim_id),
        )


def store_faucet_error(claim_id: str, error: str) -> None:
    with transaction() as connection:
        connection.execute(
            """
            UPDATE faucet_claims
            SET payout_last_error = ?,
                error_message = ?
            WHERE id = ?
            """,
            (error[:1000], error[:1000], claim_id),
        )


def mark_faucet_delivered(claim_id: str) -> None:
    with transaction() as connection:
        cursor = connection.execute(
            """
            UPDATE faucet_claims
            SET status = 'delivered',
                payout_confirmed_at = ?,
                payout_last_error = NULL,
                error_message = NULL
            WHERE id = ?
              AND status = 'payout_pending'
            """,
            (utc_now_iso(), claim_id),
        )
        if cursor.rowcount != 1:
            raise RuntimeError(
                "Faucet claim could not be marked delivered."
            )


def mark_faucet_failed(claim_id: str, error: str) -> None:
    with transaction() as connection:
        connection.execute(
            """
            UPDATE faucet_claims
            SET status = 'failed',
                payout_last_error = ?,
                error_message = ?
            WHERE id = ?
              AND status = 'payout_pending'
            """,
            (error[:1000], error[:1000], claim_id),
        )


def build_and_store_faucet_payout(
    claim: sqlite3.Row,
    settings: WorkerSettings,
    rpc: RpcPool,
) -> sqlite3.Row:
    account = Account.from_key(settings.private_key)
    recipient = Web3.to_checksum_address(claim["recipient_address"])
    amount_wei = int(claim["amount_wei"])
    reserve_wei = int(
        settings.balance_reserve_eth * Decimal(10**18)
    )

    def build(w3: Web3) -> dict[str, Any]:
        nonce = w3.eth.get_transaction_count(
            settings.payout_address,
            "pending",
        )
        latest_block = w3.eth.get_block("latest")
        base_fee = latest_block.get("baseFeePerGas")
        if base_fee is None:
            raise RuntimeError(
                "Sepolia RPC did not return baseFeePerGas."
            )

        priority_fee = int(
            settings.priority_fee_gwei * Decimal(10**9)
        )
        max_fee = (
            int(Decimal(base_fee) * settings.max_fee_multiplier)
            + priority_fee
        )
        required = (
            amount_wei
            + settings.gas_limit * max_fee
            + reserve_wei
        )
        balance = w3.eth.get_balance(settings.payout_address)

        if balance < required:
            raise RuntimeError(
                "Insufficient Sepolia ETH for faucet payout: "
                f"need at most {Web3.from_wei(required, 'ether')} ETH, "
                f"have {Web3.from_wei(balance, 'ether')} ETH."
            )

        return {
            "chainId": SEPOLIA_CHAIN_ID,
            "from": settings.payout_address,
            "to": recipient,
            "value": amount_wei,
            "nonce": nonce,
            "gas": settings.gas_limit,
            "maxFeePerGas": max_fee,
            "maxPriorityFeePerGas": priority_fee,
            "type": 2,
        }

    tx = rpc.call("build faucet payout", build)
    signed = account.sign_transaction(tx)
    raw = bytes(signed.raw_transaction)

    store_faucet_signed_transaction(
        claim["id"],
        Web3.to_hex(signed.hash),
        Web3.to_hex(raw),
        tx["nonce"],
    )

    with transaction() as connection:
        stored = connection.execute(
            "SELECT * FROM faucet_claims WHERE id = ?",
            (claim["id"],),
        ).fetchone()

    if stored is None:
        raise RuntimeError(
            "Faucet claim disappeared after storing payout."
        )

    return stored


def reconcile_faucet_payout(
    claim: sqlite3.Row,
    settings: WorkerSettings,
    rpc: RpcPool,
) -> bool:
    tx_hash = claim["payout_tx_hash"]
    raw_tx_hex = claim["payout_raw_tx"]

    if not tx_hash or not raw_tx_hex:
        raise RuntimeError(
            "Faucet payout has incomplete transaction metadata."
        )

    def get_receipt(w3: Web3):
        try:
            return w3.eth.get_transaction_receipt(tx_hash)
        except TransactionNotFound:
            return None

    receipt = rpc.call("get faucet payout receipt", get_receipt)

    if receipt is None:
        broadcast_hash = rpc.broadcast(bytes(HexBytes(raw_tx_hex)))
        if Web3.to_hex(HexBytes(broadcast_hash)).lower() != Web3.to_hex(HexBytes(tx_hash)).lower():
            raise RuntimeError(
                "Broadcast transaction hash does not match stored hash."
            )

        mark_faucet_broadcast(claim["id"])

        if claim["payout_started_at"]:
            started = datetime.fromisoformat(
                claim["payout_started_at"]
            )
            elapsed = (
                datetime.now(timezone.utc) - started
            ).total_seconds()
            if elapsed > settings.receipt_timeout_seconds:
                error = (
                    "Faucet payout still not mined after "
                    f"{int(elapsed)} seconds. Manual review required. "
                    f"Transaction: {tx_hash}"
                )
                mark_faucet_manual_review(claim["id"], error)
                LOGGER.error(error)
                return False

        LOGGER.info(
            "Faucet payout %s for claim %s is pending.",
            tx_hash,
            claim["id"],
        )
        return True

    if int(receipt["status"]) != 1:
        error = f"Sepolia faucet payout reverted: {tx_hash}"
        mark_faucet_failed(claim["id"], error)
        LOGGER.error(error)
        return False

    latest_block = rpc.call(
        "get latest Sepolia block",
        lambda w3: w3.eth.block_number,
    )
    confirmations = latest_block - receipt["blockNumber"] + 1

    if confirmations < settings.confirmations:
        LOGGER.info(
            "Faucet payout %s has %s/%s confirmations.",
            tx_hash,
            confirmations,
            settings.confirmations,
        )
        return True

    mark_faucet_delivered(claim["id"])
    LOGGER.info(
        "Faucet claim %s delivered in transaction %s.",
        claim["id"],
        tx_hash,
    )
    return False


def process_faucet_claim(
    claim: sqlite3.Row,
    settings: WorkerSettings,
    rpc: RpcPool,
) -> None:
    amount_eth = Decimal(int(claim["amount_wei"])) / Decimal(10**18)
    LOGGER.info(
        "Claimed free faucet request %s for %s Sepolia ETH.",
        claim["id"],
        amount_eth,
    )

    try:
        stored = build_and_store_faucet_payout(
            claim,
            settings,
            rpc,
        )
        rpc.broadcast(bytes(HexBytes(stored["payout_raw_tx"])))
        mark_faucet_broadcast(claim["id"])
        LOGGER.info(
            "Broadcast faucet payout %s for claim %s.",
            stored["payout_tx_hash"],
            claim["id"],
        )
    except Exception as exc:
        error = f"Could not prepare or broadcast faucet payout: {exc}"
        LOGGER.exception(error)

        with transaction() as connection:
            current = connection.execute(
                """
                SELECT payout_tx_hash, payout_attempts
                FROM faucet_claims
                WHERE id = ?
                """,
                (claim["id"],),
            ).fetchone()

        if current and current["payout_tx_hash"]:
            store_faucet_error(claim["id"], error)
        elif (
            current
            and int(current["payout_attempts"] or 0)
            >= settings.max_attempts
        ):
            mark_faucet_manual_review(claim["id"], error)
        else:
            return_faucet_to_queue(claim["id"], error)


def process_once(settings: WorkerSettings, rpc: RpcPool) -> None:
    # Never create another nonce while an earlier payout transaction from this
    # worker is unresolved. Paid orders are reconciled first, then faucet txs.
    unresolved = fetch_unresolved_payout()
    if unresolved is not None:
        try:
            reconcile_payout(unresolved, settings, rpc)
        except Exception as exc:
            error = f"Payout reconciliation failed: {exc}"
            store_payout_error(unresolved["id"], error)
            LOGGER.exception(error)
        return

    unresolved_faucet = fetch_unresolved_faucet_payout()
    if unresolved_faucet is not None:
        try:
            reconcile_faucet_payout(
                unresolved_faucet,
                settings,
                rpc,
            )
        except Exception as exc:
            error = f"Faucet payout reconciliation failed: {exc}"
            store_faucet_error(unresolved_faucet["id"], error)
            LOGGER.exception(error)
        return

    # Paid orders always have priority over free faucet requests.
    order = claim_next_order(settings.max_attempts)
    if order is not None:
        LOGGER.info(
            "Claimed order %s for %s Sepolia ETH.",
            order["id"],
            order["sepolia_amount"],
        )
        try:
            stored = build_and_store_payout(order, settings, rpc)
            rpc.broadcast(bytes(HexBytes(stored["payout_raw_tx"])))
            mark_broadcast(order["id"])
            LOGGER.info(
                "Broadcast payout %s for order %s.",
                stored["payout_tx_hash"],
                order["id"],
            )
        except Exception as exc:
            error = f"Could not prepare or broadcast payout: {exc}"
            LOGGER.exception(error)
            with transaction() as connection:
                current = connection.execute(
                    """
                    SELECT payout_tx_hash, payout_attempts
                    FROM orders
                    WHERE id = ?
                    """,
                    (order["id"],),
                ).fetchone()

            if current and current["payout_tx_hash"]:
                store_payout_error(order["id"], error)
            elif (
                current
                and int(current["payout_attempts"] or 0)
                >= settings.max_attempts
            ):
                mark_manual_review(order["id"], error)
            else:
                return_claim_to_queue(order["id"], error)
        return

    faucet_claim = claim_next_faucet(settings.max_attempts)
    if faucet_claim is not None:
        process_faucet_claim(faucet_claim, settings, rpc)


def main() -> int:
    try:
        settings = load_settings()
        configure_logging(settings.log_level)
        initialize_database()
        rpc = RpcPool(settings.rpc_urls)
    except Exception as exc:
        print(f"Payout worker startup failed: {exc}", file=sys.stderr)
        return 1

    LOGGER.info("Sepolia payout worker started for %s.", settings.payout_address)
    try:
        while True:
            process_once(settings, rpc)
            time.sleep(settings.poll_interval_seconds)
    except KeyboardInterrupt:
        LOGGER.info("Payout worker stopped.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
