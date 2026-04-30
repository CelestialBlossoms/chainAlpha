"""
Blacklist a CA: mark in DB + clean Redis Stream.

Usage:
    D:/software/anaconda/envs/py312/python.exe blacklist_ca.py <CA>
    D:/software/anaconda/envs/py312/python.exe blacklist_ca.py <CA> --unblacklist
    D:/software/anaconda/envs/py312/python.exe blacklist_ca.py <CA> --dry-run
"""
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT_DIR))

from bottom_detection.bottom_watchlist_store import set_watchlist_blacklisted, clean_redis_stream_for_ca


def main(ca):

    blacklist = "--unblacklist" not in sys.argv
    dry_run = "--dry-run" in sys.argv

    if dry_run:
        print(f"[DRY RUN] Would {'blacklist' if blacklist else 'unblacklist'}: {ca}")
        return

    # 1. Mark in DB
    set_watchlist_blacklisted(ca, blacklist)

    # 2. Clean Redis Stream (only when blacklisting)
    if blacklist:
        clean_redis_stream_for_ca(ca)


if __name__ == "__main__":
    ca ='Ha5Z2DfRv6Ar2nAeBLCGWHqzwXKL3of4DqKwzzwpump'
    main(ca)
