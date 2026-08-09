from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP


PACKAGES: tuple[tuple[int, Decimal], ...] = (
    (1, Decimal("0.20")),
    (5, Decimal("0.80")),
    (10, Decimal("1.25")),
    (25, Decimal("2.75")),
    (50, Decimal("5.00")),
    (100, Decimal("9.00")),
    (200, Decimal("16.00")),
)


def custom_unit_price(amount: int) -> Decimal:
    if amount <= 1:
        return Decimal("0.25")
    if amount <= 5:
        return Decimal("0.15")
    if amount <= 10:
        return Decimal("0.125")
    if amount <= 25:
        return Decimal("0.11")
    if amount <= 50:
        return Decimal("0.10")
    if amount <= 100:
        return Decimal("0.09")
    return Decimal("0.08")


def calculate_price(amount: int) -> Decimal:
    package_prices = dict(PACKAGES)

    if amount in package_prices:
        return package_prices[amount]

    price = Decimal(amount) * custom_unit_price(amount)
    return price.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
