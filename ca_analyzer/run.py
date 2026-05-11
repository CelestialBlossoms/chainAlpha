"""
CA Analyzer — single entry point for all token analysis.

Usage:
    D:/software/anaconda/envs/py312/python.exe ca_analyzer/run.py <CA>
    D:/software/anaconda/envs/py312/python.exe ca_analyzer/run.py <CA> --chain sol --modules full,pnl,cluster
    D:/software/anaconda/envs/py312/python.exe ca_analyzer/run.py <CA> --modules kline,pool,whales

Modules:
    full     — token info + holders + K-line + scoring          (token_full.py)
    kline    — 5m + 1h K-line analysis                          (kline.py)
    pnl      — top-100 holders & traders P&L breakdown           (pnl_breakdown.py)
    cluster  — wallet cluster & bundle detection                 (wallet_clusters.py)
    pool     — pool composition & ratio analysis                 (pool.py)
    whales   — whale/bundler deep trace                          (whales.py)
    acc      — OBV accumulation/distribution detection           (accumulation.py)
    all      — everything above (default)
"""
import argparse, subprocess, sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYTHON = "D:/software/anaconda/envs/py312/python.exe"

MODULES = {
    "full":    "token_full.py",
    "kline":   "kline.py",
    "pnl":     "pnl_breakdown.py",
    "cluster": "wallet_clusters.py",
    "pool":    "pool.py",
    "whales":  "whales.py",
    "acc":     "accumulation.py",
}


def run_script(module_name: str, script_file: str, address: str, chain: str):
    script_path = Path(__file__).resolve().parent / script_file
    if not script_path.exists():
        print(f"  [SKIP] {module_name}: {script_file} not found")
        return False

    print(f"\n{'#'*60}")
    print(f"#  MODULE: {module_name} ({script_file})")
    print(f"{'#'*60}")

    cmd = [PYTHON, str(script_path), address]
    if chain != "sol":
        cmd.extend(["--chain", chain])

    try:
        r = subprocess.run(cmd, cwd=str(ROOT), timeout=180)
        return r.returncode == 0
    except subprocess.TimeoutExpired:
        print(f"  [TIMEOUT] {module_name}")
        return False
    except Exception as e:
        print(f"  [ERROR] {module_name}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="CA Analyzer — unified token analysis")
    parser.add_argument("address", help="Token contract address")
    parser.add_argument("--chain", default="sol", help="Chain: sol/bsc/base/eth")
    parser.add_argument("--modules", default="all",
                        help="Comma-separated modules: full,kline,pnl,cluster,pool,whales,acc,all")
    args = parser.parse_args()

    chain = args.chain
    addr = args.address

    if args.modules == "all":
        selected = ["full", "pnl", "cluster", "pool"]
    else:
        selected = [m.strip() for m in args.modules.split(",") if m.strip() in MODULES]

    print(f"\n{'#'*60}")
    print(f"#  CA ANALYZER")
    print(f"#  CA: {addr}")
    print(f"#  Chain: {chain}")
    print(f"#  Modules: {', '.join(selected)}")
    print(f"{'#'*60}")

    results = {}
    for i, mod in enumerate(selected, 1):
        print(f"\n  [{i}/{len(selected)}] Running {mod}...")
        ok = run_script(mod, MODULES[mod], addr, chain)
        results[mod] = ok
        if i < len(selected):
            time.sleep(1)

    print(f"\n{'#'*60}")
    print(f"#  DONE")
    print(f"#  Results: {', '.join(f'{m}={results[m]}' for m in selected)}")
    print(f"{'#'*60}")


if __name__ == "__main__":
    main()
