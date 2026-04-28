import os

try:
    import redis
except ImportError:
    redis = None


REDIS_HOST = os.getenv("CHAIN_ALPHA_REDIS_HOST", "43.163.225.175")
REDIS_PORT = int(os.getenv("CHAIN_ALPHA_REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("CHAIN_ALPHA_REDIS_PASSWORD", "1314zxcV,1314")
REDIS_DB = int(os.getenv("CHAIN_ALPHA_REDIS_DB", "0"))
REDIS_ENABLED = os.getenv("CHAIN_ALPHA_REDIS_ENABLED", "1") != "0"
REDIS_SOCKET_TIMEOUT = float(os.getenv("CHAIN_ALPHA_REDIS_SOCKET_TIMEOUT", "2"))
REDIS_CONNECT_TIMEOUT = float(os.getenv("CHAIN_ALPHA_REDIS_CONNECT_TIMEOUT", "2"))

_CLIENT = None
_DISABLED_REASON = ""


def get_redis_client():
    global _CLIENT, _DISABLED_REASON
    if not REDIS_ENABLED:
        _DISABLED_REASON = "redis_disabled"
        return None
    if redis is None:
        _DISABLED_REASON = "redis_package_missing"
        return None
    if _CLIENT is not None:
        return _CLIENT
    try:
        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD or None,
            db=REDIS_DB,
            socket_connect_timeout=REDIS_CONNECT_TIMEOUT,
            socket_timeout=REDIS_SOCKET_TIMEOUT,
            decode_responses=True,
        )
        client.ping()
        _CLIENT = client
        return _CLIENT
    except Exception as exc:
        _DISABLED_REASON = str(exc)
        _CLIENT = None
        return None


def get_redis_disabled_reason():
    return _DISABLED_REASON


def redis_key(*parts):
    return ":".join(str(part).strip(":") for part in parts if str(part or "").strip(":"))
