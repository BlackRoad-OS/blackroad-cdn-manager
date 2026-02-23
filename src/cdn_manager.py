"""BlackRoad CDN Manager - CDN configuration and cache management tool.

Manages CDN origins, cache rules, and cache invalidation events.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

GREEN = "\033[0;32m"
RED   = "\033[0;31m"
YELLOW= "\033[1;33m"
CYAN  = "\033[0;36m"
BLUE  = "\033[0;34m"
BOLD  = "\033[1m"
NC    = "\033[0m"

DB_PATH = Path.home() / ".blackroad" / "cdn-manager.db"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class CDNOrigin:
    id: Optional[int]
    name: str
    origin_url: str
    cdn_url: str
    provider: str        # cloudflare | fastly | cloudfront | bunny
    status: str          # active | paused | error
    cache_ttl: int       # seconds
    notes: str
    created_at: Optional[str] = None
    last_purge: Optional[str] = None


@dataclass
class CacheRule:
    id: Optional[int]
    origin_id: int
    path_pattern: str    # e.g. /static/*, /api/*
    ttl: int
    cache_headers: bool
    rule_type: str       # cache | bypass | stream
    created_at: Optional[str] = None


@dataclass
class PurgeEvent:
    id: Optional[int]
    origin_id: int
    purge_type: str      # full | path | tag
    target: str
    status: str          # queued | complete | failed
    triggered_by: str
    created_at: Optional[str] = None


# ---------------------------------------------------------------------------
# Core business logic
# ---------------------------------------------------------------------------

class CDNManager:
    """Manage CDN origins, cache rules, and invalidation events."""

    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS cdn_origins (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    name       TEXT NOT NULL UNIQUE,
                    origin_url TEXT NOT NULL,
                    cdn_url    TEXT NOT NULL,
                    provider   TEXT NOT NULL DEFAULT 'cloudflare',
                    status     TEXT NOT NULL DEFAULT 'active',
                    cache_ttl  INTEGER DEFAULT 3600,
                    notes      TEXT DEFAULT '',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_purge TEXT
                );
                CREATE TABLE IF NOT EXISTS cache_rules (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    origin_id    INTEGER REFERENCES cdn_origins(id) ON DELETE CASCADE,
                    path_pattern TEXT NOT NULL,
                    ttl          INTEGER NOT NULL DEFAULT 3600,
                    cache_headers INTEGER DEFAULT 1,
                    rule_type    TEXT DEFAULT 'cache',
                    created_at   TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS purge_events (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    origin_id    INTEGER REFERENCES cdn_origins(id),
                    purge_type   TEXT NOT NULL DEFAULT 'full',
                    target       TEXT DEFAULT '*',
                    status       TEXT DEFAULT 'queued',
                    triggered_by TEXT DEFAULT 'cli',
                    created_at   TEXT DEFAULT CURRENT_TIMESTAMP
                );
            """)

    def add_origin(self, name: str, origin_url: str, cdn_url: str,
                   provider: str = "cloudflare", cache_ttl: int = 3600,
                   notes: str = "") -> CDNOrigin:
        """Register a new CDN origin."""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                """INSERT INTO cdn_origins (name, origin_url, cdn_url, provider, cache_ttl, notes)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (name, origin_url, cdn_url, provider, cache_ttl, notes),
            )
            conn.commit()
        return self._get_origin(cur.lastrowid)

    def _get_origin(self, origin_id: int) -> Optional[CDNOrigin]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM cdn_origins WHERE id = ?", (origin_id,)
            ).fetchone()
        return CDNOrigin(**dict(row)) if row else None

    def list_origins(self, provider: Optional[str] = None) -> list[CDNOrigin]:
        """List all registered CDN origins."""
        q, params = "SELECT * FROM cdn_origins", []
        if provider:
            q += " WHERE provider = ?"; params.append(provider)
        q += " ORDER BY name"
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(q, params).fetchall()
        return [CDNOrigin(**dict(r)) for r in rows]

    def add_cache_rule(self, origin_id: int, path_pattern: str,
                       ttl: int = 3600, cache_headers: bool = True,
                       rule_type: str = "cache") -> CacheRule:
        """Attach a path-based TTL override rule to an origin."""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                """INSERT INTO cache_rules (origin_id, path_pattern, ttl, cache_headers, rule_type)
                   VALUES (?, ?, ?, ?, ?)""",
                (origin_id, path_pattern, ttl, int(cache_headers), rule_type),
            )
            conn.commit()
        return CacheRule(id=cur.lastrowid, origin_id=origin_id,
                         path_pattern=path_pattern, ttl=ttl,
                         cache_headers=cache_headers, rule_type=rule_type)

    def purge_cache(self, origin_id: int, purge_type: str = "full",
                    target: str = "*", triggered_by: str = "cli") -> PurgeEvent:
        """Queue a cache purge event and stamp the origin's last_purge time."""
        ts = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                """INSERT INTO purge_events (origin_id, purge_type, target, status, triggered_by)
                   VALUES (?, ?, ?, 'queued', ?)""",
                (origin_id, purge_type, target, triggered_by),
            )
            conn.execute(
                "UPDATE cdn_origins SET last_purge = ? WHERE id = ?", (ts, origin_id)
            )
            conn.commit()
        return PurgeEvent(id=cur.lastrowid, origin_id=origin_id, purge_type=purge_type,
                          target=target, status="queued",
                          triggered_by=triggered_by, created_at=ts)

    def cdn_status(self) -> dict:
        """Return aggregate CDN health and statistics."""
        origins = self.list_origins()
        with sqlite3.connect(self.db_path) as conn:
            rule_count   = conn.execute("SELECT COUNT(*) FROM cache_rules").fetchone()[0]
            purge_total  = conn.execute("SELECT COUNT(*) FROM purge_events").fetchone()[0]
            purges_24h   = conn.execute(
                "SELECT COUNT(*) FROM purge_events WHERE created_at > datetime('now','-1 day')"
            ).fetchone()[0]
        by_provider: dict[str, int] = {}
        by_status:   dict[str, int] = {}
        for o in origins:
            by_provider[o.provider] = by_provider.get(o.provider, 0) + 1
            by_status[o.status]     = by_status.get(o.status, 0) + 1
        return {
            "total_origins":  len(origins),
            "total_rules":    rule_count,
            "total_purges":   purge_total,
            "purges_24h":     purges_24h,
            "by_provider":    by_provider,
            "by_status":      by_status,
        }

    def export_json(self, output_path: str = "cdn_export.json") -> str:
        """Export all CDN configuration to JSON."""
        origins = self.list_origins()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rules  = [dict(r) for r in conn.execute("SELECT * FROM cache_rules").fetchall()]
            events = [dict(r) for r in conn.execute(
                "SELECT * FROM purge_events ORDER BY created_at DESC LIMIT 100"
            ).fetchall()]
        payload = {
            "exported_at": datetime.now().isoformat(),
            "origins": [asdict(o) for o in origins],
            "cache_rules": rules,
            "recent_purge_events": events,
        }
        with open(output_path, "w") as fh:
            json.dump(payload, fh, indent=2)
        return output_path


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def _ttl_label(ttl: int) -> str:
    if ttl < 60:    return f"{ttl}s"
    if ttl < 3600:  return f"{ttl // 60}m"
    if ttl < 86400: return f"{ttl // 3600}h"
    return f"{ttl // 86400}d"


def _print_origin(o: CDNOrigin) -> None:
    sc = {"active": GREEN, "error": RED, "paused": YELLOW}.get(o.status, NC)
    print(f"  {BOLD}[{o.id:>3}]{NC} {CYAN}{o.name}{NC}  {BLUE}({o.provider}){NC}")
    print(f"        Status : {sc}{o.status}{NC}   TTL: {_ttl_label(o.cache_ttl)}")
    print(f"        Origin : {o.origin_url}")
    print(f"        CDN    : {o.cdn_url}")
    if o.last_purge:
        print(f"        Purged : {YELLOW}{o.last_purge[:19]}{NC}")
    if o.notes:
        print(f"        Notes  : {o.notes}")
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cdn_manager",
        description="BlackRoad CDN Manager — CDN configuration and cache management",
    )
    sub = p.add_subparsers(dest="cmd", metavar="COMMAND")

    lp = sub.add_parser("list", help="List CDN origins")
    lp.add_argument("--provider", default=None)

    ap = sub.add_parser("add", help="Register a CDN origin")
    ap.add_argument("name");       ap.add_argument("origin_url"); ap.add_argument("cdn_url")
    ap.add_argument("--provider",  default="cloudflare")
    ap.add_argument("--ttl",       type=int, default=3600, dest="cache_ttl")
    ap.add_argument("--notes",     default="")

    rp = sub.add_parser("rule", help="Add a cache rule to an origin")
    rp.add_argument("origin_id",    type=int)
    rp.add_argument("path_pattern")
    rp.add_argument("--ttl",       type=int, default=3600)
    rp.add_argument("--type",      choices=["cache", "bypass", "stream"],
                    default="cache", dest="rule_type")

    pp = sub.add_parser("purge", help="Purge cache for an origin")
    pp.add_argument("origin_id", type=int)
    pp.add_argument("--type",    choices=["full", "path", "tag"], default="full", dest="purge_type")
    pp.add_argument("--target",  default="*")
    pp.add_argument("--by",      default="cli", dest="triggered_by")

    sub.add_parser("status", help="Show CDN fleet summary")

    ep = sub.add_parser("export", help="Export configuration to JSON")
    ep.add_argument("--output", default="cdn_export.json")

    return p


def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()
    mgr    = CDNManager()
    print(f"\n{BOLD}{BLUE}╔══ BlackRoad CDN Manager ══╗{NC}\n")

    if args.cmd == "list":
        origins = mgr.list_origins(provider=getattr(args, "provider", None))
        if not origins:
            print(f"  {YELLOW}No origins registered.{NC}\n"); return
        print(f"  {BOLD}CDN Origins ({len(origins)}){NC}\n")
        for o in origins:
            _print_origin(o)

    elif args.cmd == "add":
        o = mgr.add_origin(args.name, args.origin_url, args.cdn_url,
                           args.provider, args.cache_ttl, args.notes)
        print(f"  {GREEN}✓ Origin registered: [{o.id}] {o.name}{NC}\n")

    elif args.cmd == "rule":
        r = mgr.add_cache_rule(args.origin_id, args.path_pattern,
                               args.ttl, rule_type=args.rule_type)
        print(f"  {GREEN}✓ Rule [{r.id}]: {args.path_pattern} → TTL {_ttl_label(args.ttl)}{NC}\n")

    elif args.cmd == "purge":
        ev = mgr.purge_cache(args.origin_id, args.purge_type, args.target, args.triggered_by)
        print(f"  {GREEN}✓ Purge queued (event #{ev.id}){NC}")
        print(f"  {YELLOW}Origin #{args.origin_id}  type={args.purge_type}  target={args.target}{NC}\n")

    elif args.cmd == "status":
        s = mgr.cdn_status()
        print(f"  {BOLD}CDN Fleet Status{NC}")
        print(f"  {'Origins':<22}  {CYAN}{s['total_origins']}{NC}")
        print(f"  {'Cache Rules':<22}  {s['total_rules']}")
        print(f"  {'Total Purges':<22}  {s['total_purges']}")
        print(f"  {'Purges (24 h)':<22}  {YELLOW}{s['purges_24h']}{NC}")
        if s["by_provider"]:
            print(f"\n  {BOLD}By Provider:{NC}")
            for prov, n in sorted(s["by_provider"].items()):
                print(f"    {CYAN}{prov:<20}{NC} {n}")
        print()

    elif args.cmd == "export":
        path = mgr.export_json(args.output)
        print(f"  {GREEN}✓ Exported to: {path}{NC}\n")

    else:
        parser.print_help(); print()


if __name__ == "__main__":
    main()
