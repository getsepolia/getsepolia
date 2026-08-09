from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


OrderStatus = Literal[
    "created",
    "payment_submitted",
    "payment_confirmed",
    "payout_pending",
    "delivered",
    "manual_review",
    "failed",
    "expired",
]


class PricingPackage(BaseModel):
    amount: int
    price: Decimal


class PricingResponse(BaseModel):
    currency: str = "USDC"
    network: str = "arbitrum"
    packages: list[PricingPackage]
    custom_min: int
    custom_max: int
    daily_limit_per_wallet: int


class ServiceStatusResponse(BaseModel):
    service: str
    environment: str
    payment_enabled: bool
    min_order_eth: int
    max_order_eth: int
    daily_limit_per_wallet: int
    database: str


class OrderCreate(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    sepolia_amount: int = Field(ge=1, le=500)
    recipient_address: str
    payment_wallet: str | None = None

    @field_validator("recipient_address")
    @classmethod
    def validate_recipient_address(cls, value: str) -> str:
        return cls._validate_evm_address(value)

    @field_validator("payment_wallet")
    @classmethod
    def validate_payment_wallet(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return cls._validate_evm_address(value)

    @staticmethod
    def _validate_evm_address(value: str) -> str:
        if not value.startswith("0x"):
            value = f"0x{value}"

        if len(value) != 42:
            raise ValueError("Address must contain 20 bytes.")

        try:
            int(value[2:], 16)
        except ValueError as exc:
            raise ValueError("Address must be hexadecimal.") from exc

        return value.lower()


class OrderResponse(BaseModel):
    id: str
    created_at: datetime
    expires_at: datetime
    recipient_address: str
    payment_wallet: str | None = None
    sepolia_amount: int
    price_usdc: Decimal
    status: OrderStatus
    payment_tx_hash: str | None = None
    payout_tx_hash: str | None = None
    error_message: str | None = None


class OrderStatusUpdate(BaseModel):
    status: OrderStatus
    error_message: str | None = Field(
        default=None,
        max_length=1000,
    )
