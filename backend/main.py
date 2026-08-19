from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import os
import sqlite3
import uuid
import socket
import time
import threading
from collections import defaultdict, deque
import urllib.error
import urllib.request
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_FLOOR
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator

from .config import get_settings
from .database import initialize_database, transaction
from .orders import (
    assign_payment_wallet,
    create_order,
    get_order,
    update_order_status,
)
from .pricing import PACKAGES
from .schemas import (
    OrderCreate,
    OrderResponse,
    OrderStatusUpdate,
    PricingPackage,
    PricingResponse,
    ServiceStatusResponse,
)


settings = get_settings()

PROJECT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_DIR / ".env", override=False)
FRONTEND_DIR = PROJECT_DIR / "frontend"

SEPOLIA_CHAIN_ID = 11155111
ARBITRUM_CHAIN_ID = 42161
ERC20_TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa"
    "952ba7f163c4a11628f55a4df523b3ef"
)

INVENTORY_RESERVATION_STATUSES = (
    "created",
    "payment_submitted",
    "payment_confirmed",
    "payout_pending",
    "manual_review",
)

FAUCET_RESERVATION_STATUSES = (
    "queued",
    "payout_pending",
    "manual_review",
)


class InventoryResponse(BaseModel):
    network: str = "sepolia"
    chain_id: int = SEPOLIA_CHAIN_ID
    payout_address: str
    wallet_balance_eth: Decimal
    reserved_eth: Decimal
    gas_reserve_eth: Decimal
    available_eth: Decimal
    available_order_eth: int
    max_order_eth: int


class PaymentConfigResponse(BaseModel):
    payment_enabled: bool
    chain_id: int = ARBITRUM_CHAIN_ID
    network: str = "Arbitrum One"
    currency: str = "USDC"
    token_contract: str
    treasury_address: str
    token_decimals: int = 6
    required_confirmations: int


class PaymentWalletAssignment(BaseModel):
    payment_wallet: str

    @field_validator("payment_wallet")
    @classmethod
    def validate_payment_wallet(cls, value: str) -> str:
        return _validate_user_address(value, "payment_wallet")


class FaucetClaimRequest(BaseModel):
    recipient_address: str

    @field_validator("recipient_address")
    @classmethod
    def validate_recipient_address(cls, value: str) -> str:
        return _validate_user_address(value, "recipient_address")


class FaucetConfigResponse(BaseModel):
    enabled: bool
    amount_eth: Decimal
    cooldown_hours: int


class FaucetClaimResponse(BaseModel):
    id: str
    status: str
    recipient_address: str
    amount_eth: Decimal
    created_at: datetime
    next_claim_at: datetime
    payout_tx_hash: str | None = None
    payout_broadcast_at: datetime | None = None
    payout_confirmed_at: datetime | None = None
    error_message: str | None = None


class PaymentSubmission(BaseModel):
    transaction_hash: str = Field(min_length=66, max_length=66)

    @field_validator("transaction_hash")
    @classmethod
    def validate_transaction_hash(cls, value: str) -> str:
        value = value.strip().lower()
        if not value.startswith("0x"):
            raise ValueError("Transaction hash must start with 0x.")
        try:
            int(value[2:], 16)
        except ValueError as exc:
            raise ValueError("Transaction hash must be hexadecimal.") from exc
        return value


@asynccontextmanager
async def lifespan(_: FastAPI):
    initialize_database()
    yield


app = FastAPI(
    title=settings.app_name,
    version="0.8.0",
    lifespan=lifespan,
)


# Lightweight per-process sliding-window rate limiter for public API endpoints.
# Uvicorn currently runs a single worker. If multiple API workers are used
# later, move this state to a shared store such as Redis.
_rate_limit_buckets: dict[str, deque[float]] = defaultdict(deque)
_rate_limit_lock = threading.Lock()
_RATE_LIMIT_MAX_KEYS = 10000


def _enforce_rate_limit(
    request: Request,
    *,
    scope: str,
    limit: int,
    window_seconds: int,
) -> None:
    now = time.monotonic()
    cutoff = now - window_seconds
    client_ip = _client_ip(request)
    key = f"{scope}:{client_ip}"

    with _rate_limit_lock:
        bucket = _rate_limit_buckets[key]

        while bucket and bucket[0] <= cutoff:
            bucket.popleft()

        if len(bucket) >= limit:
            retry_after = max(
                1,
                int(window_seconds - (now - bucket[0])),
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail={
                    "code": "rate_limit_exceeded",
                    "message": "Too many requests. Please try again later.",
                },
                headers={"Retry-After": str(retry_after)},
            )

        bucket.append(now)

        # Keep the transient in-memory key set bounded.
        if len(_rate_limit_buckets) > _RATE_LIMIT_MAX_KEYS:
            stale_keys = [
                bucket_key
                for bucket_key, timestamps in _rate_limit_buckets.items()
                if not timestamps or timestamps[-1] <= cutoff
            ]
            for bucket_key in stale_keys:
                _rate_limit_buckets.pop(bucket_key, None)


def _validate_user_address(value: str, field_name: str) -> str:
    value = value.strip().lower()
    if not value.startswith("0x"):
        value = f"0x{value}"

    if len(value) != 42:
        raise ValueError(f"{field_name} is not a valid EVM address.")

    try:
        int(value[2:], 16)
    except ValueError as exc:
        raise ValueError(
            f"{field_name} is not a valid EVM address."
        ) from exc

    return value


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "configuration_missing",
                "message": f"Required environment variable {name} is missing.",
            },
        )
    return value


def _normalize_address(value: str, variable_name: str) -> str:
    value = value.strip().lower()
    if not value.startswith("0x"):
        value = f"0x{value}"
    if len(value) != 42:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "invalid_configuration",
                "message": f"{variable_name} is not a valid EVM address.",
            },
        )
    try:
        int(value[2:], 16)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "invalid_configuration",
                "message": f"{variable_name} is not a valid EVM address.",
            },
        ) from exc
    return value


def _rpc_timeout_seconds() -> int:
    raw = os.getenv("RPC_TIMEOUT_SECONDS", "12").strip()
    try:
        timeout = int(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "invalid_configuration",
                "message": "RPC_TIMEOUT_SECONDS must be an integer.",
            },
        ) from exc

    if timeout < 1 or timeout > 120:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "invalid_configuration",
                "message": "RPC_TIMEOUT_SECONDS must be between 1 and 120.",
            },
        )
    return timeout


def _rpc_urls(
    primary_variable: str,
    backup_variable: str,
) -> list[str]:
    urls: list[str] = []

    primary = os.getenv(primary_variable, "").strip()
    if primary:
        urls.append(primary)

    backups = os.getenv(backup_variable, "")
    for item in backups.replace("\\n", ",").split(","):
        url = item.strip()
        if url:
            urls.append(url)

    unique_urls = list(dict.fromkeys(urls))

    if not unique_urls:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "configuration_missing",
                "message": (
                    f"Configure {primary_variable} and/or "
                    f"{backup_variable}."
                ),
            },
        )

    return unique_urls


def _rpc_call_single(
    rpc_url: str,
    method: str,
    params: list[Any],
    *,
    timeout: int,
) -> Any:
    request_body = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
    ).encode("utf-8")

    request = urllib.request.Request(
        rpc_url,
        data=request_body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Sepolia-Market/0.6.1",
            "Connection": "close",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (
        urllib.error.URLError,
        urllib.error.HTTPError,
        TimeoutError,
        socket.timeout,
        json.JSONDecodeError,
    ) as exc:
        raise RuntimeError(
            f"RPC transport failure for {method}: {exc}"
        ) from exc

    if payload.get("error"):
        error = payload["error"]
        message = (
            error.get("message")
            if isinstance(error, dict)
            else str(error)
        )
        raise RuntimeError(
            message or f"RPC method {method} returned an error."
        )

    if "result" not in payload:
        raise RuntimeError(
            f"RPC method {method} returned no result field."
        )

    return payload["result"]


def _rpc_call_network(
    rpc_urls: list[str],
    expected_chain_id: int,
    method: str,
    params: list[Any],
) -> Any:
    timeout = _rpc_timeout_seconds()
    errors: list[str] = []

    for index, rpc_url in enumerate(rpc_urls, start=1):
        try:
            chain_result = _rpc_call_single(
                rpc_url,
                "eth_chainId",
                [],
                timeout=timeout,
            )
            chain_id = int(chain_result, 16)

            if chain_id != expected_chain_id:
                errors.append(
                    f"RPC #{index}: wrong chain ID {chain_id}"
                )
                continue

            return _rpc_call_single(
                rpc_url,
                method,
                params,
                timeout=timeout,
            )
        except Exception as exc:
            error_message = f"RPC #{index}: {type(exc).__name__}: {exc}"
            print(
                f"[RPC failover] {method}: {error_message}",
                flush=True,
            )
            errors.append(error_message)

    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail={
            "code": "all_rpcs_unavailable",
            "message": (
                f"All configured RPC endpoints failed for {method}."
            ),
            "attempts": errors,
        },
    )


def _sepolia_rpc_urls() -> list[str]:
    return _rpc_urls(
        "SEPOLIA_RPC_URL",
        "SEPOLIA_RPC_BACKUP_URLS",
    )


def _arbitrum_rpc_urls() -> list[str]:
    return _rpc_urls(
        "ARBITRUM_RPC_URL",
        "ARBITRUM_RPC_BACKUP_URLS",
    )


def _sepolia_balance_wei() -> tuple[str, int]:
    payout_address = _normalize_address(
        _required_env("SEPOLIA_PAYOUT_ADDRESS"),
        "SEPOLIA_PAYOUT_ADDRESS",
    )

    result = _rpc_call_network(
        _sepolia_rpc_urls(),
        SEPOLIA_CHAIN_ID,
        "eth_getBalance",
        [payout_address, "latest"],
    )
    return payout_address, int(result, 16)


def _gas_reserve_eth() -> Decimal:
    raw = os.getenv("SEPOLIA_GAS_RESERVE_ETH", "0.05").strip()
    try:
        reserve = Decimal(raw)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "invalid_configuration",
                "message": "SEPOLIA_GAS_RESERVE_ETH must be a decimal number.",
            },
        ) from exc
    if reserve < 0:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "invalid_configuration",
                "message": "SEPOLIA_GAS_RESERVE_ETH may not be negative.",
            },
        )
    return reserve


def _reserved_inventory_eth() -> Decimal:
    now_iso = datetime.now(timezone.utc).isoformat()
    placeholders = ",".join(
        "?" for _ in INVENTORY_RESERVATION_STATUSES
    )
    faucet_placeholders = ",".join(
        "?" for _ in FAUCET_RESERVATION_STATUSES
    )

    with transaction() as connection:
        connection.execute(
            """
            UPDATE orders
            SET status = 'expired'
            WHERE status = 'created'
              AND expires_at <= ?
            """,
            (now_iso,),
        )

        order_row = connection.execute(
            f"""
            SELECT COALESCE(SUM(sepolia_amount), 0) AS total
            FROM orders
            WHERE status IN ({placeholders})
            """,
            INVENTORY_RESERVATION_STATUSES,
        ).fetchone()

        faucet_row = connection.execute(
            f"""
            SELECT COALESCE(SUM(amount_wei), 0) AS total_wei
            FROM faucet_claims
            WHERE status IN ({faucet_placeholders})
            """,
            FAUCET_RESERVATION_STATUSES,
        ).fetchone()

    order_reserved = Decimal(int(order_row["total"] or 0))
    faucet_reserved = (
        Decimal(int(faucet_row["total_wei"] or 0))
        / Decimal(10**18)
    )
    return order_reserved + faucet_reserved


def _inventory_snapshot() -> InventoryResponse:
    payout_address, balance_wei = _sepolia_balance_wei()
    wallet_balance = Decimal(balance_wei) / Decimal(10**18)
    reserved = _reserved_inventory_eth()
    gas_reserve = _gas_reserve_eth()

    available = wallet_balance - reserved - gas_reserve
    if available < 0:
        available = Decimal("0")

    available_order_eth = int(
        available.to_integral_value(rounding=ROUND_FLOOR)
    )
    configured_max = int(settings.custom_max)
    effective_max = min(configured_max, available_order_eth)

    return InventoryResponse(
        payout_address=payout_address,
        wallet_balance_eth=wallet_balance,
        reserved_eth=reserved,
        gas_reserve_eth=gas_reserve,
        available_eth=available,
        available_order_eth=available_order_eth,
        max_order_eth=effective_max,
    )


def _ensure_inventory_for_order(amount: int) -> None:
    inventory = _inventory_snapshot()
    if amount > inventory.available_order_eth:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "insufficient_inventory",
                "message": (
                    "The requested Sepolia ETH amount is no longer available."
                ),
                "requested_eth": amount,
                "available_order_eth": inventory.available_order_eth,
            },
        )



def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "1" if default else "0").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail={
            "code": "invalid_configuration",
            "message": f"{name} must be a boolean value.",
        },
    )


def _faucet_amount_eth() -> Decimal:
    raw = os.getenv("FAUCET_AMOUNT_ETH", "0.02").strip()
    try:
        amount = Decimal(raw)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "invalid_configuration",
                "message": "FAUCET_AMOUNT_ETH must be a decimal number.",
            },
        ) from exc

    if not amount.is_finite() or amount <= 0:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "invalid_configuration",
                "message": "FAUCET_AMOUNT_ETH must be greater than zero.",
            },
        )
    return amount


def _faucet_cooldown_hours() -> int:
    raw = os.getenv("FAUCET_COOLDOWN_HOURS", "24").strip()
    try:
        hours = int(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "invalid_configuration",
                "message": "FAUCET_COOLDOWN_HOURS must be an integer.",
            },
        ) from exc

    if hours < 1 or hours > 24 * 30:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "invalid_configuration",
                "message": "FAUCET_COOLDOWN_HOURS must be between 1 and 720.",
            },
        )
    return hours


def _faucet_daily_limit_eth() -> Decimal:
    raw = os.getenv("FAUCET_DAILY_LIMIT_ETH", "1.0").strip()
    try:
        limit = Decimal(raw)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "invalid_configuration",
                "message": "FAUCET_DAILY_LIMIT_ETH must be a decimal number.",
            },
        ) from exc

    if not limit.is_finite() or limit <= 0:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "invalid_configuration",
                "message": "FAUCET_DAILY_LIMIT_ETH must be greater than zero.",
            },
        )
    return limit


def _eth_to_wei(amount: Decimal) -> int:
    return int((amount * Decimal(10**18)).to_integral_value())


def _client_ip(request: Request) -> str:
    if request.client is None or not request.client.host:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "client_ip_unavailable",
                "message": "The client address could not be determined.",
            },
        )

    raw = request.client.host.strip()
    try:
        return ipaddress.ip_address(raw).compressed
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "client_ip_invalid",
                "message": "The client address could not be normalized.",
            },
        ) from exc


def _faucet_ip_hash(request: Request) -> str:
    secret = _required_env("FAUCET_IP_HASH_SECRET").encode("utf-8")
    if len(secret) < 32:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "invalid_configuration",
                "message": "FAUCET_IP_HASH_SECRET must contain at least 32 characters.",
            },
        )

    return hmac.new(
        secret,
        _client_ip(request).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _next_claim_at(created_at: str, cooldown: timedelta) -> datetime:
    return datetime.fromisoformat(created_at) + cooldown


def _raise_faucet_cooldown(
    *,
    scope: str,
    created_at: str,
    cooldown: timedelta,
) -> None:
    next_claim_at = _next_claim_at(created_at, cooldown)
    raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail={
            "code": "faucet_cooldown",
            "message": (
                "A free Sepolia faucet claim has already been accepted "
                "within the cooldown period."
            ),
            "scope": scope,
            "next_claim_at": next_claim_at.isoformat(),
        },
        headers={
            "Retry-After": str(
                max(
                    1,
                    int(
                        (
                            next_claim_at - datetime.now(timezone.utc)
                        ).total_seconds()
                    ),
                )
            )
        },
    )


def _create_faucet_claim(
    request: Request,
    recipient_address: str,
) -> FaucetClaimResponse:
    if not _env_bool("FAUCET_ENABLED", True):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "faucet_disabled",
                "message": "The free Sepolia faucet is currently disabled.",
            },
        )

    amount_eth = _faucet_amount_eth()
    amount_wei = _eth_to_wei(amount_eth)
    cooldown = timedelta(hours=_faucet_cooldown_hours())
    daily_limit_wei = _eth_to_wei(_faucet_daily_limit_eth())
    ip_hash = _faucet_ip_hash(request)
    recipient_address = _validate_user_address(
        recipient_address,
        "recipient_address",
    )

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    cutoff_iso = (now - cooldown).isoformat()
    rolling_day_cutoff = (now - timedelta(hours=24)).isoformat()

    # Fetch the chain balance before taking the SQLite write lock. The
    # reservation check itself is then repeated atomically inside the DB tx.
    _, balance_wei = _sepolia_balance_wei()
    gas_reserve_wei = _eth_to_wei(_gas_reserve_eth())

    with transaction() as connection:
        ip_row = connection.execute(
            """
            SELECT created_at
            FROM faucet_claims
            WHERE ip_hash = ?
              AND created_at > ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (ip_hash, cutoff_iso),
        ).fetchone()
        if ip_row is not None:
            _raise_faucet_cooldown(
                scope="network",
                created_at=ip_row["created_at"],
                cooldown=cooldown,
            )

        recipient_row = connection.execute(
            """
            SELECT created_at
            FROM faucet_claims
            WHERE recipient_address = ?
              AND created_at > ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (recipient_address, cutoff_iso),
        ).fetchone()
        if recipient_row is not None:
            _raise_faucet_cooldown(
                scope="recipient",
                created_at=recipient_row["created_at"],
                cooldown=cooldown,
            )

        daily_row = connection.execute(
            """
            SELECT COALESCE(SUM(amount_wei), 0) AS total_wei
            FROM faucet_claims
            WHERE created_at > ?
            """,
            (rolling_day_cutoff,),
        ).fetchone()
        used_today_wei = int(daily_row["total_wei"] or 0)

        if used_today_wei + amount_wei > daily_limit_wei:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail={
                    "code": "faucet_daily_budget_exhausted",
                    "message": (
                        "The free faucet has reached its rolling 24-hour "
                        "distribution limit. Please try again later."
                    ),
                },
            )

        placeholders = ",".join(
            "?" for _ in INVENTORY_RESERVATION_STATUSES
        )
        order_row = connection.execute(
            f"""
            SELECT COALESCE(SUM(sepolia_amount), 0) AS total
            FROM orders
            WHERE status IN ({placeholders})
            """,
            INVENTORY_RESERVATION_STATUSES,
        ).fetchone()
        reserved_orders_wei = int(order_row["total"] or 0) * 10**18

        faucet_placeholders = ",".join(
            "?" for _ in FAUCET_RESERVATION_STATUSES
        )
        faucet_row = connection.execute(
            f"""
            SELECT COALESCE(SUM(amount_wei), 0) AS total_wei
            FROM faucet_claims
            WHERE status IN ({faucet_placeholders})
            """,
            FAUCET_RESERVATION_STATUSES,
        ).fetchone()
        reserved_faucet_wei = int(faucet_row["total_wei"] or 0)

        available_wei = (
            balance_wei
            - gas_reserve_wei
            - reserved_orders_wei
            - reserved_faucet_wei
        )

        if available_wei < amount_wei:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "insufficient_inventory",
                    "message": (
                        "The free faucet is temporarily unavailable because "
                        "there is not enough unreserved Sepolia ETH."
                    ),
                },
            )

        claim_id = uuid.uuid4().hex
        connection.execute(
            """
            INSERT INTO faucet_claims (
                id,
                created_at,
                recipient_address,
                ip_hash,
                amount_wei,
                status
            )
            VALUES (?, ?, ?, ?, ?, 'queued')
            """,
            (
                claim_id,
                now_iso,
                recipient_address,
                ip_hash,
                amount_wei,
            ),
        )

    return FaucetClaimResponse(
        id=claim_id,
        status="queued",
        recipient_address=recipient_address,
        amount_eth=amount_eth,
        created_at=now,
        next_claim_at=now + cooldown,
        payout_tx_hash=None,
        payout_broadcast_at=None,
        payout_confirmed_at=None,
        error_message=None,
    )


def _get_faucet_claim(claim_id: str) -> FaucetClaimResponse:
    with transaction() as connection:
        row = connection.execute(
            '''
            SELECT
                id,
                created_at,
                recipient_address,
                amount_wei,
                status,
                payout_tx_hash,
                payout_broadcast_at,
                payout_confirmed_at,
                error_message
            FROM faucet_claims
            WHERE id = ?
            ''',
            (claim_id,),
        ).fetchone()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "code": "faucet_claim_not_found",
                "message": "Faucet claim not found.",
            },
        )

    created_at = datetime.fromisoformat(row["created_at"])
    amount_eth = Decimal(int(row["amount_wei"])) / Decimal(10**18)

    payout_broadcast_at = (
        datetime.fromisoformat(row["payout_broadcast_at"])
        if row["payout_broadcast_at"]
        else None
    )
    payout_confirmed_at = (
        datetime.fromisoformat(row["payout_confirmed_at"])
        if row["payout_confirmed_at"]
        else None
    )

    return FaucetClaimResponse(
        id=row["id"],
        status=row["status"],
        recipient_address=row["recipient_address"],
        amount_eth=amount_eth,
        created_at=created_at,
        next_claim_at=created_at
        + timedelta(hours=_faucet_cooldown_hours()),
        payout_tx_hash=row["payout_tx_hash"],
        payout_broadcast_at=payout_broadcast_at,
        payout_confirmed_at=payout_confirmed_at,
        error_message=row["error_message"],
    )


def _payment_config() -> PaymentConfigResponse:
    token_contract = _normalize_address(
        _required_env("ARBITRUM_USDC_CONTRACT"),
        "ARBITRUM_USDC_CONTRACT",
    )
    treasury_address = _normalize_address(
        _required_env("USDC_TREASURY_ADDRESS"),
        "USDC_TREASURY_ADDRESS",
    )

    try:
        required_confirmations = int(
            os.getenv("ARBITRUM_CONFIRMATIONS", "1")
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "invalid_configuration",
                "message": "ARBITRUM_CONFIRMATIONS must be an integer.",
            },
        ) from exc

    if required_confirmations < 1:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "invalid_configuration",
                "message": "ARBITRUM_CONFIRMATIONS must be at least 1.",
            },
        )

    return PaymentConfigResponse(
        payment_enabled=bool(settings.payment_enabled),
        token_contract=token_contract,
        treasury_address=treasury_address,
        required_confirmations=required_confirmations,
    )


def _topic_address(topic: str) -> str:
    return f"0x{topic[-40:]}".lower()


def _verify_usdc_payment(
    order: OrderResponse,
    transaction_hash: str,
) -> tuple[str, int, int]:
    rpc_urls = _arbitrum_rpc_urls()

    usdc_contract = _normalize_address(
        _required_env("ARBITRUM_USDC_CONTRACT"),
        "ARBITRUM_USDC_CONTRACT",
    )

    treasury = _normalize_address(
        _required_env("USDC_TREASURY_ADDRESS"),
        "USDC_TREASURY_ADDRESS",
    )

    receipt = _rpc_call_network(
        rpc_urls,
        ARBITRUM_CHAIN_ID,
        "eth_getTransactionReceipt",
        [transaction_hash],
    )

    if receipt is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "payment_not_mined",
                "message": (
                    "The payment transaction has not been mined yet."
                ),
            },
        )

    if int(receipt.get("status", "0x0"), 16) != 1:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "payment_reverted",
                "message": "The payment transaction reverted.",
            },
        )

    expected_sender = (
        order.payment_wallet.lower()
        if order.payment_wallet
        else None
    )

    expected_amount = int(
        (
            Decimal(order.price_usdc)
            * Decimal(10**6)
        ).to_integral_value()
    )

    matching_transfers: list[tuple[str, int]] = []

    for log in receipt.get("logs", []):
        if log.get("address", "").lower() != usdc_contract:
            continue

        topics = log.get("topics", [])

        if len(topics) < 3:
            continue

        if topics[0].lower() != ERC20_TRANSFER_TOPIC:
            continue

        try:
            sender = _topic_address(topics[1])
            recipient = _topic_address(topics[2])
            amount = int(log.get("data", "0x0"), 16)
        except (TypeError, ValueError, IndexError):
            continue

        if recipient != treasury:
            continue

        if amount != expected_amount:
            continue

        if (
            expected_sender is not None
            and sender != expected_sender
        ):
            continue

        matching_transfers.append(
            (sender, amount)
        )

    if not matching_transfers:
        if expected_sender is not None:
            message = (
                "No matching native USDC Transfer event was found for "
                "the assigned payment wallet, treasury and exact "
                "order amount."
            )
        else:
            message = (
                "No matching native USDC Transfer event was found for "
                "the treasury and exact order amount."
            )

        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "payment_transfer_mismatch",
                "message": message,
            },
        )

    unique_senders = {
        sender
        for sender, _ in matching_transfers
    }

    if expected_sender is None and len(unique_senders) != 1:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "ambiguous_payment_sender",
                "message": (
                    "The transaction contains multiple matching USDC "
                    "transfers from different senders. The payment "
                    "sender cannot be determined unambiguously."
                ),
            },
        )

    payment_sender = (
        expected_sender
        if expected_sender is not None
        else matching_transfers[0][0]
    )

    latest_block = int(
        _rpc_call_network(
            rpc_urls,
            ARBITRUM_CHAIN_ID,
            "eth_blockNumber",
            [],
        ),
        16,
    )

    transaction_block = int(
        receipt["blockNumber"],
        16,
    )

    # Anti-replay protection: a payment must have been mined during
    # this order's lifetime.  A transaction from before the order was
    # created must never be accepted as proof of payment, even if its
    # sender, recipient, token contract and amount all match.
    payment_block = _rpc_call_network(
        rpc_urls,
        ARBITRUM_CHAIN_ID,
        "eth_getBlockByNumber",
        [receipt["blockNumber"], False],
    )

    if payment_block is None or not payment_block.get("timestamp"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "payment_block_unavailable",
                "message": (
                    "The payment block could not be verified. "
                    "Please try again shortly."
                ),
            },
        )

    try:
        payment_time = datetime.fromtimestamp(
            int(payment_block["timestamp"], 16),
            tz=timezone.utc,
        )
        order_created_at = order.created_at
        order_expires_at = order.expires_at

        if order_created_at.tzinfo is None:
            order_created_at = order_created_at.replace(tzinfo=timezone.utc)
        else:
            order_created_at = order_created_at.astimezone(timezone.utc)

        if order_expires_at.tzinfo is None:
            order_expires_at = order_expires_at.replace(tzinfo=timezone.utc)
        else:
            order_expires_at = order_expires_at.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "payment_block_invalid",
                "message": "The payment block timestamp could not be verified.",
            },
        ) from exc

    # EVM block timestamps have one-second resolution, while created_at
    # contains microseconds.  Flooring the order timestamp avoids rejecting
    # a legitimate payment mined in the same second as order creation.
    order_created_floor = order_created_at.replace(microsecond=0)

    if payment_time < order_created_floor:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "payment_predates_order",
                "message": (
                    "This payment transaction predates the order and "
                    "cannot be used as payment for it."
                ),
            },
        )

    if payment_time > order_expires_at:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "payment_after_expiry",
                "message": (
                    "This payment transaction was mined after the order "
                    "expired and cannot be used for it."
                ),
            },
        )

    confirmations = (
        latest_block
        - transaction_block
        + 1
    )

    if confirmations < 1:
        confirmations = 0

    return (
        payment_sender,
        expected_amount,
        confirmations,
    )


def _store_verified_payment(
    order_id: str,
    transaction_hash: str,
    payment_sender: str,
    confirmations: int,
) -> OrderResponse:
    required_confirmations = int(
        os.getenv("ARBITRUM_CONFIRMATIONS", "1")
    )
    target_status = (
        "payment_confirmed"
        if confirmations >= required_confirmations
        else "payment_submitted"
    )

    try:
        with transaction() as connection:
            existing = connection.execute(
                "SELECT id FROM orders WHERE payment_tx_hash = ?",
                (transaction_hash,),
            ).fetchone()

            if existing is not None and existing["id"] != order_id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "payment_already_used",
                        "message": (
                            "This payment transaction is already assigned "
                            "to another order."
                        ),
                    },
                )

            row = connection.execute(
                """
                SELECT status, payment_tx_hash, payment_wallet
                FROM orders
                WHERE id = ?
                """,
                (order_id,),
            ).fetchone()

            if row is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={
                        "code": "order_not_found",
                        "message": "Order not found.",
                    },
                )

            if row["status"] not in {
                "created",
                "payment_submitted",
                "payment_confirmed",
            }:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "order_not_payable",
                        "message": (
                            f"Order status '{row['status']}' does not "
                            "accept a payment submission."
                        ),
                    },
                )

            if (
                row["payment_tx_hash"]
                and row["payment_tx_hash"] != transaction_hash
            ):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "different_payment_already_attached",
                        "message": (
                            "A different payment transaction is already "
                            "attached to this order."
                        ),
                    },
                )

            if (
                row["payment_wallet"]
                and row["payment_wallet"].lower()
                != payment_sender.lower()
            ):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "payment_wallet_mismatch",
                        "message": (
                            "The verified USDC sender does not match the "
                            "payment wallet attached to this order."
                        ),
                    },
                )

            connection.execute(
                """
                UPDATE orders
                SET payment_wallet = COALESCE(payment_wallet, ?),
                    payment_tx_hash = ?,
                    status = ?,
                    error_message = NULL
                WHERE id = ?
                """,
                (
                    payment_sender,
                    transaction_hash,
                    target_status,
                    order_id,
                ),
            )
    except sqlite3.IntegrityError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "payment_already_used",
                "message": (
                    "This payment transaction is already assigned "
                    "to an order."
                ),
            },
        ) from exc

    return get_order(order_id)


@app.get(
    f"{settings.api_prefix}/status",
    response_model=ServiceStatusResponse,
)
def api_status() -> ServiceStatusResponse:
    return ServiceStatusResponse(
        service=settings.service_status,
        environment=settings.app_environment,
        payment_enabled=settings.payment_enabled,
        min_order_eth=settings.custom_min,
        max_order_eth=settings.custom_max,
        daily_limit_per_wallet=settings.daily_limit_per_wallet,
        database="sqlite",
    )


@app.get(
    f"{settings.api_prefix}/inventory",
    response_model=InventoryResponse,
)
def api_inventory() -> InventoryResponse:
    return _inventory_snapshot()


@app.get(
    f"{settings.api_prefix}/faucet-config",
    response_model=FaucetConfigResponse,
)
def api_faucet_config() -> FaucetConfigResponse:
    return FaucetConfigResponse(
        enabled=_env_bool("FAUCET_ENABLED", True),
        amount_eth=_faucet_amount_eth(),
        cooldown_hours=_faucet_cooldown_hours(),
    )


@app.post(
    f"{settings.api_prefix}/faucet",
    response_model=FaucetClaimResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
def api_faucet_claim(
    payload: FaucetClaimRequest,
    request: Request,
) -> FaucetClaimResponse:
    _enforce_rate_limit(
        request,
        scope="faucet_claim",
        limit=10,
        window_seconds=600,
    )
    return _create_faucet_claim(
        request,
        payload.recipient_address,
    )


@app.get(
    f"{settings.api_prefix}/faucet/{{claim_id}}",
    response_model=FaucetClaimResponse,
)
def api_get_faucet_claim(
    claim_id: str,
    request: Request,
) -> FaucetClaimResponse:
    _enforce_rate_limit(
        request,
        scope="faucet_status",
        limit=120,
        window_seconds=60,
    )
    return _get_faucet_claim(claim_id)


@app.get(
    f"{settings.api_prefix}/payment-config",
    response_model=PaymentConfigResponse,
)
def api_payment_config() -> PaymentConfigResponse:
    return _payment_config()


@app.get(
    f"{settings.api_prefix}/pricing",
    response_model=PricingResponse,
)
def api_pricing() -> PricingResponse:
    return PricingResponse(
        packages=[
            PricingPackage(amount=amount, price=price)
            for amount, price in PACKAGES
        ],
        custom_min=settings.custom_min,
        custom_max=settings.custom_max,
        daily_limit_per_wallet=settings.daily_limit_per_wallet,
    )


@app.post(
    f"{settings.api_prefix}/orders",
    response_model=OrderResponse,
    status_code=status.HTTP_201_CREATED,
)
def api_create_order(
    payload: OrderCreate,
    request: Request,
) -> OrderResponse:
    _enforce_rate_limit(
        request,
        scope="order_create",
        limit=30,
        window_seconds=600,
    )

    if payload.sepolia_amount < int(settings.custom_min):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "amount_below_minimum",
                "message": (
                    f"Minimum order is {settings.custom_min} Sepolia ETH."
                ),
            },
        )

    if payload.sepolia_amount > int(settings.custom_max):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "code": "amount_above_maximum",
                "message": (
                    f"Maximum order is {settings.custom_max} Sepolia ETH."
                ),
            },
        )

    _ensure_inventory_for_order(payload.sepolia_amount)
    return create_order(payload)


@app.get(
    f"{settings.api_prefix}/orders/{{order_id}}",
    response_model=OrderResponse,
)
def api_get_order(
    order_id: str,
    request: Request,
) -> OrderResponse:
    _enforce_rate_limit(
        request,
        scope="order_status",
        limit=120,
        window_seconds=60,
    )
    return get_order(order_id)


@app.post(
    f"{settings.api_prefix}/orders/{{order_id}}/payment-wallet",
    response_model=OrderResponse,
)
def api_assign_payment_wallet(
    order_id: str,
    payload: PaymentWalletAssignment,
    request: Request,
) -> OrderResponse:
    _enforce_rate_limit(
        request,
        scope="payment_wallet",
        limit=30,
        window_seconds=600,
    )

    if not settings.payment_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "payments_disabled",
                "message": "Payment processing is currently disabled.",
            },
        )

    current = get_order(order_id)

    if current.status != "created":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "payment_wallet_locked",
                "message": (
                    "The payment wallet can only be assigned while "
                    "the order is in status 'created'."
                ),
            },
        )

    requested_wallet = payload.payment_wallet.lower()

    if current.payment_wallet is not None:
        if current.payment_wallet.lower() != requested_wallet:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "different_payment_wallet_attached",
                    "message": (
                        "A different payment wallet is already attached "
                        "to this order."
                    ),
                },
            )
        return current

    assigned = assign_payment_wallet(
        order_id,
        requested_wallet,
    )

    # Fail closed if a concurrent request won the one-time assignment race.
    if (
        assigned.payment_wallet is None
        or assigned.payment_wallet.lower() != requested_wallet
    ):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "payment_wallet_assignment_race",
                "message": (
                    "The payment wallet was assigned concurrently. "
                    "Reload the order before continuing."
                ),
            },
        )

    return assigned


@app.post(
    f"{settings.api_prefix}/orders/{{order_id}}/payment",
    response_model=OrderResponse,
)
def api_submit_payment(
    order_id: str,
    payload: PaymentSubmission,
    request: Request,
) -> OrderResponse:
    _enforce_rate_limit(
        request,
        scope="payment_verify",
        limit=20,
        window_seconds=600,
    )

    if not settings.payment_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "payments_disabled",
                "message": "Payment processing is currently disabled.",
            },
        )

    order = get_order(order_id)

    if order.status == "expired":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "order_expired",
                "message": "The order expired before payment verification.",
            },
        )

    payment_sender, _, confirmations = _verify_usdc_payment(
        order,
        payload.transaction_hash,
    )

    return _store_verified_payment(
        order_id,
        payload.transaction_hash,
        payment_sender,
        confirmations,
    )


@app.post(
    f"{settings.api_prefix}/orders/{{order_id}}/status",
    response_model=OrderResponse,
)
def api_update_order_status(
    order_id: str,
    payload: OrderStatusUpdate,
) -> OrderResponse:
    if not settings.enable_dev_status_endpoint:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "code": "dev_status_endpoint_disabled",
                "message": (
                    "The development status endpoint is disabled."
                ),
            },
        )

    return update_order_status(
        order_id=order_id,
        new_status=payload.status,
        error_message=payload.error_message,
    )


if not FRONTEND_DIR.is_dir():
    raise RuntimeError(
        f"Frontend directory does not exist: {FRONTEND_DIR}"
    )

app.mount(
    "/",
    StaticFiles(directory=FRONTEND_DIR, html=True),
    name="frontend",
)
