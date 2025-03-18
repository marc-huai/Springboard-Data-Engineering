from random import choices, randint
from string import ascii_letters, digits

account_chars = digits + ascii_letters

def _random_account_id() -> str:
    return "".join(choices(account_chars, k=12))

def _random_amount() -> float:
    return randint(100, 1000000) / 100

def create_random_transaction() -> dict:
    return {
        "source": _random_account_id(),
        "target": _random_account_id(),
        "amount": _random_amount(),
        "currency": "USD",
    }
