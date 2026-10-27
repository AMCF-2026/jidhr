"""Reconcile pending event_map rows HubSpot already has (V5.96). Map-only.

    python scripts/event_map_reconcile.py           # preview
    python scripts/event_map_reconcile.py --apply   # write event_map only

See sync/event_map_reconcile.py for the rules.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main(argv=None) -> int:
    from sync import event_map_reconcile as recon

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    try:
        out = recon.run(apply=args.apply)
    except recon.Refused as e:
        print(f"Event map reconcile did not run: {e}", file=sys.stderr)
        return 2
    print(recon.report(out))
    return 1 if out.get("stopped") else 0


if __name__ == "__main__":
    sys.exit(main())
