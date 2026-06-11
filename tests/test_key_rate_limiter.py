import uuid

from utils.key_rate_limiter import KeyRateLimiter


def _unique_scope(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def test_key_rate_limiter_shares_state_within_scope():
    scope = _unique_scope("shared")
    limiter1 = KeyRateLimiter(["k1", "k2"], 5, 60, scope=scope)
    limiter2 = KeyRateLimiter(["k1", "k2"], 5, 60, scope=scope)

    assert limiter1.lock is limiter2.lock
    assert limiter1.history is limiter2.history


def test_key_rate_limiter_does_not_share_state_across_scopes():
    limiter1 = KeyRateLimiter(["k1", "k2"], 5, 60, scope=_unique_scope("scope_a"))
    limiter2 = KeyRateLimiter(["k1", "k2"], 5, 60, scope=_unique_scope("scope_b"))

    assert limiter1.lock is not limiter2.lock
    assert limiter1.history is not limiter2.history


def test_block_key_skips_temporarily_blocked_key():
    scope = _unique_scope("block")
    limiter = KeyRateLimiter(["k1", "k2"], 100, 60, scope=scope)

    limiter.block_key("k1", 60)

    assert limiter.acquire_key() == "k2"

