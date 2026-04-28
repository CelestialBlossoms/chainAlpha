import argparse
import json
from pathlib import Path
from typing import Any


DEFAULT_INPUT = "package.json"
DEFAULT_OUTPUT = "gmgn_outputs/package_wallet_map.json"


def extract_json_array(text: str) -> list[dict[str, Any]]:
    try:
        data = json.loads(text)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
    except json.JSONDecodeError:
        pass

    start = text.find("\n[")
    if start >= 0:
        start += 1
    else:
        start = text.find("[")
    end = text.rfind("]")
    if start < 0 or end < start:
        raise ValueError("input does not contain a JSON array")

    data = json.loads(text[start : end + 1])
    if not isinstance(data, list):
        raise ValueError("extracted JSON is not an array")
    return [item for item in data if isinstance(item, dict)]


def wallet_items_to_map(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    wallet_map = {}
    for item in items:
        address = str(item.get("address") or "").strip()
        if not address:
            continue
        groups = item.get("groups") or []
        if not isinstance(groups, list):
            groups = [str(groups)]
        wallet_map[address] = {
            "name": str(item.get("name") or ""),
            "groups": [str(group) for group in groups if str(group).strip()],
        }
    return wallet_map


def convert_package_wallets(input_path: str = DEFAULT_INPUT, output_path: str = DEFAULT_OUTPUT) -> dict[str, dict[str, Any]]:
    text = Path(input_path).read_text(encoding="utf-8")
    items = extract_json_array(text)
    wallet_map = wallet_items_to_map(items)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(wallet_map, ensure_ascii=False, indent=2), encoding="utf-8")
    return wallet_map


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert package wallet list to address keyed map.")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Input JSON file, default package.json.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output JSON file.")
    args = parser.parse_args()

    wallet_map = convert_package_wallets(args.input, args.output)
    print(f"converted {len(wallet_map)} wallets -> {args.output}")


if __name__ == "__main__":
    main()
