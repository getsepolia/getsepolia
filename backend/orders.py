from __future__ import annotations

import uuid
from datetime import timedelta
from decimal import Decimal

from fastapi import HTTPException, status

from .config import get_settings
from .database import transaction, utc_now
from .pricing import calculate_price
from .schemas import OrderCreate, OrderResponse


ACTIVE_DAILY_LIMIT_STATUSES = (
    "created",
    "payment_submitted",
    "payment_confirmed",
    "payout_pending",
    "delivered",
    "manual_review",
)


def _row_to_order(row) -> OrderResponse:
    return OrderResponse(
        id=row["id"],
        created_at=row["created_at"],
        expires_at=row["expires_at"],
        recipient_address=row["recipient_address"],
        payment_wallet=row["payment_wallet"],
        sepolia_amount=row["sepolia_amount"],
        price_usdc=Decimal(row["price_usdc"]),
        status=row["status"],
        payment_tx_hash=row["payment_tx_hash"],
        payout_tx_hash=row["payout_tx_hash"],
        error_message=row["error_message"],
    )


def expire_stale_orders(connection) -> None:
    now_iso = utc_now().isoformat()
    connection.execute(
        """
        UPDATE orders
        SET status = 'expired'
        WHERE status = 'created'
          AND expires_at <= ?
        """,
        (now_iso,),
    )


def _daily_wallet_total(
    connection,
    payment_wallet: str,
    *,
    exclude_order_id: str | None = None,
) -> int:
    now = utc_now()
    day_start = now.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    placeholders = ",".join(
        "?" for _ in ACTIVE_DAILY_LIMIT_STATUSES
    )

    parameters: list[object] = [
        payment_wallet.lower(),
        day_start.isoformat(),
        *ACTIVE_DAILY_LIMIT_STATUSES,
    ]

    exclusion_sql = ""
    if exclude_order_id is not None:
        exclusion_sql = " AND id != ?"
        parameters.append(exclude_order_id)

    row = connection.execute(
        f"""
        SELECT COALESCE(SUM(sepolia_amount), 0) AS total
        FROM orders
        WHERE payment_wallet = ?
          AND created_at >= ?
          AND status IN ({placeholders})
          {exclusion_sql}
        """,
        parameters,
    ).fetchone()

    return int(row["total"])


def create_order(payload: OrderCreate) -> OrderResponse:
    settings = get_settings()
    now = utc_now()
    expires_at = now + timedelta(
        minutes=settings.order_expiry_minutes
    )

    wallet = (
        payload.payment_wallet.lower()
        if payload.payment_wallet
        else None
    )
    recipient = payload.recipient_address.lower()
    price = calculate_price(payload.sepolia_amount)
    order_id = f"ord_{uuid.uuid4().hex}"

    with transaction() as connection:
        expire_stale_orders(connection)

        # Backward compatibility: if a caller still creates an order with
        # a payment wallet, enforce the daily limit immediately.
        if wallet is not None:
            ordered_today = _daily_wallet_total(
                connection,
                wallet,
            )

            if (
                ordered_today + payload.sepolia_amount
                > settings.daily_limit_per_wallet
            ):
                remaining = max(
                    0,
                    settings.daily_limit_per_wallet - ordered_today,
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "daily_limit_exceeded",
                        "message": (
                            "The daily order limit for this payment wallet "
                            "would be exceeded."
                        ),
                        "daily_limit": settings.daily_limit_per_wallet,
                        "remaining": remaining,
                    },
                )

        connection.execute(
            """
            INSERT INTO orders (
                id,
                created_at,
                expires_at,
                recipient_address,
                payment_wallet,
                sepolia_amount,
                price_usdc,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'created')
            """,
            (
                order_id,
                now.isoformat(),
                expires_at.isoformat(),
                recipient,
                wallet,
                payload.sepolia_amount,
                format(price, "f"),
            ),
        )

        row = connection.execute(
            "SELECT * FROM orders WHERE id = ?",
            (order_id,),
        ).fetchone()

    return _row_to_order(row)


def assign_payment_wallet(
    order_id: str,
    payment_wallet: str,
) -> OrderResponse:
    settings = get_settings()
    payment_wallet = payment_wallet.lower()
    now_iso = utc_now().isoformat()

    with transaction() as connection:
        expire_stale_orders(connection)

        row = connection.execute(
            """
            SELECT *
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

        if row["status"] == "expired" or row["expires_at"] <= now_iso:
            if row["status"] == "created":
                connection.execute(
                    """
                    UPDATE orders
                    SET status = 'expired'
                    WHERE id = ?
                    """,
                    (order_id,),
                )

            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "order_expired",
                    "message": "The order expired before payment.",
                },
            )

        if row["status"] != "created":
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

        existing_wallet = row["payment_wallet"]

        if existing_wallet:
            if existing_wallet.lower() != payment_wallet:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "different_payment_wallet_attached",
                        "message": (
                            "A different payment wallet is already "
                            "attached to this order."
                        ),
                    },
                )

            return _row_to_order(row)

        ordered_today = _daily_wallet_total(
            connection,
            payment_wallet,
            exclude_order_id=order_id,
        )

        order_amount = int(row["sepolia_amount"])

        if (
            ordered_today + order_amount
            > settings.daily_limit_per_wallet
        ):
            remaining = max(
                0,
                settings.daily_limit_per_wallet - ordered_today,
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "daily_limit_exceeded",
                    "message": (
                        "The daily order limit for this payment wallet "
                        "would be exceeded."
                    ),
                    "daily_limit": settings.daily_limit_per_wallet,
                    "remaining": remaining,
                },
            )

        connection.execute(
            """
            UPDATE orders
            SET payment_wallet = ?
            WHERE id = ?
              AND status = 'created'
              AND payment_wallet IS NULL
            """,
            (
                payment_wallet,
                order_id,
            ),
        )

        updated = connection.execute(
            "SELECT * FROM orders WHERE id = ?",
            (order_id,),
        ).fetchone()

    return _row_to_order(updated)


def get_order(order_id: str) -> OrderResponse:
    with transaction() as connection:
        expire_stale_orders(connection)
        row = connection.execute(
            "SELECT * FROM orders WHERE id = ?",
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

    return _row_to_order(row)


def list_orders(
    payment_wallet: str | None = None,
    limit: int = 20,
) -> list[OrderResponse]:
    limit = min(max(limit, 1), 100)

    with transaction() as connection:
        expire_stale_orders(connection)

        if payment_wallet:
            rows = connection.execute(
                """
                SELECT *
                FROM orders
                WHERE payment_wallet = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (payment_wallet.lower(), limit),
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT *
                FROM orders
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    return [_row_to_order(row) for row in rows]


ALLOWED_STATUS_TRANSITIONS: dict[str, set[str]] = {
    "created": {
        "payment_submitted",
        "manual_review",
        "failed",
        "expired",
    },
    "payment_submitted": {
        "payment_confirmed",
        "manual_review",
        "failed",
        "expired",
    },
    "payment_confirmed": {
        "payout_pending",
        "manual_review",
        "failed",
    },
    "payout_pending": {
        "delivered",
        "manual_review",
        "failed",
    },
    "manual_review": {
        "payment_submitted",
        "payment_confirmed",
        "payout_pending",
        "delivered",
        "failed",
        "expired",
    },
    "delivered": set(),
    "failed": set(),
    "expired": set(),
}


def update_order_status(
    order_id: str,
    new_status: str,
    error_message: str | None = None,
) -> OrderResponse:
    with transaction() as connection:
        expire_stale_orders(connection)

        row = connection.execute(
            "SELECT * FROM orders WHERE id = ?",
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

        current_status = row["status"]

        if new_status == current_status:
            return _row_to_order(row)

        allowed = ALLOWED_STATUS_TRANSITIONS.get(
            current_status,
            set(),
        )

        if new_status not in allowed:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "invalid_status_transition",
                    "message": (
                        f"Status transition from '{current_status}' "
                        f"to '{new_status}' is not allowed."
                    ),
                    "current_status": current_status,
                    "requested_status": new_status,
                    "allowed_statuses": sorted(allowed),
                },
            )

        stored_error = error_message

        if new_status not in {
            "manual_review",
            "failed",
        }:
            stored_error = None

        connection.execute(
            """
            UPDATE orders
            SET status = ?,
                error_message = ?
            WHERE id = ?
            """,
            (
                new_status,
                stored_error,
                order_id,
            ),
        )

        updated = connection.execute(
            "SELECT * FROM orders WHERE id = ?",
            (order_id,),
        ).fetchone()

    return _row_to_order(updated)
