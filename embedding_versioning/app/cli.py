import argparse
from app.db import init_db
from app.pipeline import build_version
from app.eval import evaluate_version, shadow_compare, get_active_version
from app.promote import promote

def main():
    init_db()

    p = argparse.ArgumentParser(prog="embedding_versioning")
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build")
    b.add_argument("--version", required=True)

    e = sub.add_parser("eval")
    e.add_argument("--version", required=True)

    se = sub.add_parser("shadow-eval")
    se.add_argument("--candidate", required=True)

    pr = sub.add_parser("promote")
    pr.add_argument("--version", required=True)
    pr.add_argument("--require-shadow-pass", action="store_true")

    av = sub.add_parser("active")

    args = p.parse_args()

    if args.cmd == "build":
        print(build_version(args.version))
    elif args.cmd == "eval":
        print(evaluate_version(args.version))
    elif args.cmd == "shadow-eval":
        print(shadow_compare(args.candidate))
    elif args.cmd == "promote":
        print(promote(args.version, require_shadow_pass=args.require_shadow_pass))
    elif args.cmd == "active":
        print({"active_version": get_active_version()})

if __name__ == "__main__":
    main()
