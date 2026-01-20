from __future__ import annotations

import argparse
import json
import sys

from parallelhassnes.scenarios.runner import run_scenarios


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="parallelhassnes.scenarios")
    parser.add_argument("--json", action="store_true", help="Emit JSON result")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))

    res = run_scenarios()
    if args.json:
        print(json.dumps(res, ensure_ascii=False, indent=2))
    else:
        if res.get("ok"):
            print("OK")
        else:
            print("FAILED")
            for e in res.get("errors", []):
                print("- " + str(e))
    return 0 if res.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())

