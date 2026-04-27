#!/usr/bin/env python3
"""
Audit and optionally fix post lengths in delayed_posts.json and posts_status.json.

Usage examples:
  # Dry run (no changes):
  python3 audit_posts.py --limit 1000

  # Only audit delayed_posts.json
  python3 audit_posts.py --limit 1000 --targets delayed

  # Apply fixes (with timestamped backups)
  python3 audit_posts.py --limit 1000 --fix

    # Delete items over the limit (with timestamped backups)
    python3 audit_posts.py --limit 1000 --delete-over-limit --targets delayed

Options:
  --limit N             Character limit (default: 1000)
  --targets T [T ...]   One or more of: delayed, status (default: both)
  --fix                 Write back truncated posts and create backups
  --cwd PATH            Directory containing the JSON files (default: current dir)
"""
import argparse
import json
import os
import re
import shutil
import sys
from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple

DELAYED_FILENAME = "delayed_posts.json"
STATUS_FILENAME = "posts_status.json"

FOOTER_KEYWORDS = (
    "More details:",
    "Apply now",
    "Post your vacancy",
    "➡️ Post your vacancy",
)


def load_json(path: str) -> Any:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def backup_file(path: str) -> str:
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = f"{path}.bak.{ts}"
    shutil.copy2(path, backup_path)
    return backup_path


def detect_footer(text: str) -> Tuple[str, str]:
    """
    Try to split the post into (body, footer), where footer contains the usual
    lines like More details / Apply now / Post your vacancy. If not found, footer is ''.
    """
    lines = text.splitlines()
    # Find the first line from the end that contains a footer keyword
    footer_start_idx = None
    for i in range(len(lines) - 1, -1, -1):
        li = lines[i]
        if any(k in li for k in FOOTER_KEYWORDS):
            footer_start_idx = i
            break
    if footer_start_idx is None:
        return text, ""

    # Include one blank line above footer block if present
    start = footer_start_idx
    if start - 1 >= 0 and lines[start - 1].strip() == "":
        start -= 1

    body_lines = lines[:start]
    footer_lines = lines[start:]
    return ("\n".join(body_lines).rstrip(), "\n".join(footer_lines).lstrip())


def smart_truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text

    body, footer = detect_footer(text)
    # If there is a footer, try to preserve it; else fall back to simple truncation
    if footer:
        # Reserve space for footer and a newline between body and footer
        reserve = len(footer) + 1
        if reserve >= limit:
            # Footer itself is too long; hard truncate full text
            return text[:limit]

        # Allow space for an ellipsis if truncation is needed
        allow = limit - reserve
        if len(body) <= allow:
            # No need to truncate body, compose back
            candidate = (body + "\n" + footer).strip()
            return candidate[:limit]

        # Truncate body at a word boundary
        truncated = body[:max(0, allow - 1)]  # keep room for ellipsis
        # Prefer cutting at last whitespace to avoid mid-word cut
        cut_at = truncated.rstrip().rfind(" ")
        if cut_at >= 0 and cut_at > allow // 2:
            truncated = truncated[:cut_at].rstrip()
        truncated = truncated.rstrip() + "…"
        out = (truncated + "\n" + footer)
        return out[:limit]
    else:
        # Simple truncation with ellipsis
        if limit <= 1:
            return text[:limit]
        base = text[: limit - 1]
        # Try to cut on whitespace if possible
        cut_at = base.rstrip().rfind(" ")
        if cut_at >= 0 and cut_at > (limit // 2):
            base = base[:cut_at].rstrip()
        return (base + "…")[:limit]


def audit_items(items: Iterable[Tuple[str, str]], limit: int) -> Tuple[int, int, List[Tuple[str, int]]]:
    """
    items: iterable of (id, text)
    returns: (total, over_limit_count, offenders[(id, length)])
    """
    total = 0
    over = 0
    offenders: List[Tuple[str, int]] = []
    for iid, text in items:
        total += 1
        l = len(text or "")
        if l > limit:
            over += 1
            offenders.append((iid, l))
    offenders.sort(key=lambda x: x[1], reverse=True)
    return total, over, offenders


def iter_delayed(path: str) -> Iterable[Tuple[str, str]]:
    data = load_json(path)
    if not isinstance(data, list):
        return []
    out: List[Tuple[str, str]] = []
    for obj in data:
        if not isinstance(obj, dict):
            continue
        iid = str(obj.get("job_id", ""))
        text = obj.get("text", "")
        out.append((iid, text))
    return out


def iter_status(path: str) -> Iterable[Tuple[str, str]]:
    data = load_json(path)
    if not isinstance(data, dict):
        return []
    out: List[Tuple[str, str]] = []
    for iid, obj in data.items():
        if not isinstance(obj, dict):
            continue
        text = obj.get("text", "")
        out.append((str(iid), text))
    return out


def apply_fixes_delayed(path: str, limit: int) -> Tuple[int, int]:
    data = load_json(path)
    if not isinstance(data, list):
        return (0, 0)
    changed = 0
    examined = 0
    for obj in data:
        if not isinstance(obj, dict):
            continue
        examined += 1
        t = obj.get("text", "")
        if isinstance(t, str) and len(t) > limit:
            obj["text"] = smart_truncate(t, limit)
            changed += 1
    if changed:
        backup_file(path)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    return examined, changed


def apply_fixes_status(path: str, limit: int) -> Tuple[int, int]:
    data = load_json(path)
    if not isinstance(data, dict):
        return (0, 0)
    changed = 0
    examined = 0
    for iid, obj in data.items():
        if not isinstance(obj, dict):
            continue
        examined += 1
        t = obj.get("text", "")
        if isinstance(t, str) and len(t) > limit:
            obj["text"] = smart_truncate(t, limit)
            changed += 1
    if changed:
        backup_file(path)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    return examined, changed


def delete_over_limit_delayed(path: str, limit: int) -> Tuple[int, int]:
    """Delete list items in delayed_posts.json whose text exceeds limit.
    Returns (examined, deleted)."""
    data = load_json(path)
    if not isinstance(data, list):
        return (0, 0)
    examined = 0
    kept: List[Dict[str, Any]] = []
    for obj in data:
        if not isinstance(obj, dict):
            continue
        examined += 1
        t = obj.get("text", "")
        if isinstance(t, str) and len(t) <= limit:
            kept.append(obj)
    deleted = examined - len(kept)
    if deleted > 0:
        backup_file(path)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(kept, f, ensure_ascii=False, indent=2)
    return examined, deleted


def delete_over_limit_status(path: str, limit: int) -> Tuple[int, int]:
    """Delete dict items in posts_status.json whose text exceeds limit.
    Returns (examined, deleted)."""
    data = load_json(path)
    if not isinstance(data, dict):
        return (0, 0)
    examined = 0
    kept: Dict[str, Any] = {}
    for iid, obj in data.items():
        if not isinstance(obj, dict):
            continue
        examined += 1
        t = obj.get("text", "")
        if isinstance(t, str) and len(t) <= limit:
            kept[iid] = obj
    deleted = examined - len(kept)
    if deleted > 0:
        backup_file(path)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(kept, f, ensure_ascii=False, indent=2)
    return examined, deleted


def main():
    ap = argparse.ArgumentParser(description="Audit and fix post lengths in JSON stores.")
    ap.add_argument("--limit", type=int, default=1000)
    ap.add_argument("--targets", nargs="+", choices=["delayed", "status"], default=["delayed", "status"])
    ap.add_argument("--fix", action="store_true", help="Apply truncation and write back with backups")
    ap.add_argument("--delete-over-limit", action="store_true", help="Delete items whose text exceeds limit (backup before writing)")
    ap.add_argument("--cwd", type=str, default=os.getcwd())
    args = ap.parse_args()

    if args.fix and args.delete_over_limit:
        print("Error: --fix and --delete-over-limit are mutually exclusive.")
        return 2

    base = os.path.abspath(args.cwd)
    delayed_path = os.path.join(base, DELAYED_FILENAME)
    status_path = os.path.join(base, STATUS_FILENAME)

    had_any = False

    if "delayed" in args.targets:
        had_any = True
        items = list(iter_delayed(delayed_path))
        total, over, offenders = audit_items(items, args.limit)
        print(f"[delayed_posts.json] total={total}, over_limit={over} (limit={args.limit})")
        if offenders:
            top = offenders[:5]
            print("  top offenders:")
            for iid, l in top:
                print(f"    id={iid} len={l}")
        if args.delete_over_limit and os.path.exists(delayed_path):
            examined, deleted = delete_over_limit_delayed(delayed_path, args.limit)
            print(f"  delete applied: examined={examined}, deleted={deleted}")
        elif args.fix and os.path.exists(delayed_path):
            examined, changed = apply_fixes_delayed(delayed_path, args.limit)
            print(f"  fix applied: examined={examined}, changed={changed}")

    if "status" in args.targets:
        had_any = True
        items = list(iter_status(status_path))
        total, over, offenders = audit_items(items, args.limit)
        print(f"[posts_status.json] total={total}, over_limit={over} (limit={args.limit})")
        if offenders:
            top = offenders[:5]
            print("  top offenders:")
            for iid, l in top:
                print(f"    id={iid} len={l}")
        if args.delete_over_limit and os.path.exists(status_path):
            examined, deleted = delete_over_limit_status(status_path, args.limit)
            print(f"  delete applied: examined={examined}, deleted={deleted}")
        elif args.fix and os.path.exists(status_path):
            examined, changed = apply_fixes_status(status_path, args.limit)
            print(f"  fix applied: examined={examined}, changed={changed}")

    if not had_any:
        print("No targets selected.")
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
