"""
Discord LoL Stats Bot — Feature Upgrade (PUUID-first)
Adds:
  • Progress & History:
      - rank snapshots for Solo and Flex
      - LP delta utility and '/lol_progress' (last N days)
      - background watcher: pings channel on tier/division changes and LP movement
  • Profile stats:
      - '/lol_profile' shows level, profile icon url, total mastery score,
        and Top 3 champions by mastery (names via DDragon)
  • Leaderboard & Climb race:
      - '/lol_leaderboard' (top N by Solo rank/LP)
      - '/lol_climb_race' (biggest LP gains over last N days)
  • Friendly banter:
      - '/lol_compare' side-by-side rank comparison
  • Weekly digest (NEW):
      - Every Monday 09:00 America/Toronto (configurable)
      - Shows each player's current Solo rank + LP Δ7d
      - Shows rank change over the week (e.g., Gold I → Plat IV)
      - Lists players with no ranked Solo activity in 7 days

Notes:
  - Uses /lol/league/v4/entries/by-puuid for ranks (no Summoner ID needed).
  - Uses Summoner-V4 by-puuid only to probe platform and read level/profile icon.
  - Champion names fetched from Data Dragon (no API key required).
"""
from __future__ import annotations

import os
import asyncio
import logging
import sqlite3
import aiohttp
import discord
from discord import app_commands
from discord.ext import tasks
from dotenv import load_dotenv, find_dotenv
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any
from zoneinfo import ZoneInfo

# ------------- Config -------------
HERE = Path(__file__).parent
ENV_FILE = os.getenv("ENV_FILE", "bot.env")
ENV_PATH = HERE / ENV_FILE
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)
else:
    alt = HERE / ".env"
    if alt.exists():
        load_dotenv(alt)
    else:
        load_dotenv(find_dotenv())


DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
RIOT_API_KEY = os.getenv("RIOT_API_KEY", "")
GUILD_ID = os.getenv("GUILD_ID")
ALERT_CHANNEL_ID = int(os.getenv("ALERT_CHANNEL_ID", "1417546827015651504"))
WATCH_INTERVAL_MIN = int(os.getenv("WATCH_INTERVAL_MIN", "10"))  # watcher cadence

DEFAULT_PLATFORM = "na1"
DEFAULT_REGIONAL = "americas"

# Weekly report schedule (local time for America/Toronto by default)
WEEKLY_TZ = os.getenv("WEEKLY_TZ", "America/Toronto")
WEEKLY_DAY = int(os.getenv("WEEKLY_DAY", "0"))   # 0=Mon … 6=Sun
WEEKLY_HOUR = int(os.getenv("WEEKLY_HOUR", "9"))  # hour in 24h

ALL_PLATFORMS = [
    "na1","euw1","eun1","kr","br1","la1","la2","oc1","tr1","ru","jp1"
]
PLATFORM_TO_REGIONAL = {
    "na1": "americas", "br1": "americas", "la1": "americas", "la2": "americas", "oc1": "americas",
    "euw1": "europe", "eun1": "europe", "tr1": "europe", "ru": "europe",
    "kr": "asia", "jp1": "asia",
}

def regional_from_platform(plat: str) -> str:
    return PLATFORM_TO_REGIONAL.get(plat, "americas")

# ------------- Logging -------------
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("lolbot")

# ------------- DB Layer -------------
DB_DIR = Path(os.getenv("DB_DIR", HERE / "data"))
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / os.getenv("DB_FILENAME", "lolbot.sqlite")

def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

SCHEMA_SQL = r"""
CREATE TABLE IF NOT EXISTS players (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  discord_id TEXT,
  game_name TEXT NOT NULL,
  tag_line TEXT NOT NULL,
  puuid TEXT NOT NULL,
  summoner_id TEXT NOT NULL,       -- kept for compatibility; we store PUUID here too
  platform TEXT NOT NULL DEFAULT 'na1',
  UNIQUE(game_name, tag_line, platform)
);

CREATE TABLE IF NOT EXISTS rank_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  player_id INTEGER NOT NULL,
  ts_utc INTEGER NOT NULL,
  queue_type TEXT NOT NULL,        -- 'RANKED_SOLO_5x5' or 'RANKED_FLEX_SR'
  tier TEXT,
  division TEXT,
  lp INTEGER,
  wins INTEGER,
  losses INTEGER,
  games_ranked_solo INTEGER,
  FOREIGN KEY(player_id) REFERENCES players(id)
);

CREATE INDEX IF NOT EXISTS idx_rank_snapshots_pid_time ON rank_snapshots(player_id, ts_utc);

-- Track which ISO week got a digest already
CREATE TABLE IF NOT EXISTS weekly_reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  week_iso INTEGER NOT NULL,
  year_iso INTEGER NOT NULL,
  posted_ts_utc INTEGER NOT NULL,
  UNIQUE(week_iso, year_iso)
);
"""
with db_connect() as c:
    c.executescript(SCHEMA_SQL)
    c.commit()

# ------------- Riot API -------------
class RiotHTTP:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def _get(self, url: str, params: Dict | None = None, retry: int = 2):
        if params is None:
            params = {}
        params["api_key"] = os.getenv("RIOT_API_KEY", "")

        for attempt in range(retry + 1):
            async with self.session.get(url, params=params, timeout=30) as resp:
                if 200 <= resp.status < 300:
                    ct = resp.headers.get("Content-Type", "")
                    return await (resp.json() if "application/json" in ct else resp.text())

                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "2"))
                    log.warning(f"429 rate limit. Sleeping {retry_after}s.")
                    await asyncio.sleep(retry_after)
                    continue

                if resp.status == 404:
                    return None

                text = await resp.text()
                if attempt < retry:
                    await asyncio.sleep(1 + attempt)
                    continue
                raise RuntimeError(f"Riot API error {resp.status}: {text[:200]}")

    # Account / Summoner / Leagues
    async def by_riot_id(self, regional: str, game_name: str, tag_line: str) -> Optional[Dict]:
        url = f"https://{regional}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
        return await self._get(url)

    async def by_puuid_summoner_v4(self, puuid: str, platform: str) -> Optional[Dict]:
        url = f"https://{platform}.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}"
        return await self._get(url)

    async def league_entries_by_puuid(self, puuid: str, platform: str) -> Optional[List[Dict]]:
        url = f"https://{platform}.api.riotgames.com/lol/league/v4/entries/by-puuid/{puuid}"
        return await self._get(url)

    # Champion Mastery (by PUUID)
    async def mastery_top(self, puuid: str, platform: str, count: int = 3) -> Optional[List[Dict]]:
        url = f"https://{platform}.api.riotgames.com/lol/champion-mastery/v4/champion-masteries/by-puuid/{puuid}/top?count={count}"
        return await self._get(url)

    async def mastery_score(self, puuid: str, platform: str) -> Optional[int]:
        url = f"https://{platform}.api.riotgames.com/lol/champion-mastery/v4/scores/by-puuid/{puuid}"
        return await self._get(url)

    # Match-V5 (optional WR calc)
    async def match_ids(self, puuid: str, regional: str, queue: Optional[int] = 420, count: int = 20) -> Optional[List[str]]:
        params = {"start": 0, "count": count}
        if queue is not None:
            params["queue"] = queue
        url = f"https://{regional}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        return await self._get(url, params=params)

    async def match(self, match_id: str, regional: str) -> Optional[Dict]:
        url = f"https://{regional}.api.riotgames.com/lol/match/v5/matches/{match_id}"
        return await self._get(url)

    # DDragon (champion names mapping)
    async def ddragon_latest_version(self) -> str:
        url = "https://ddragon.leagueoflegends.com/api/versions.json"
        arr = await self._get(url, params={})
        return arr[0] if isinstance(arr, list) and arr else "14.10.1"

    async def ddragon_champion_map(self) -> Dict[str, Any]:
        ver = await self.ddragon_latest_version()
        url = f"https://ddragon.leagueoflegends.com/cdn/{ver}/data/en_US/champion.json"
        data = await self._get(url, params={})
        m: Dict[int, str] = {}
        for champ_name, info in (data.get("data", {}) or {}).items():
            try:
                m[int(info["key"])] = champ_name
            except Exception:
                continue
        return m

# ------------- Helpers -------------
TIERS_ORDER = ["IRON","BRONZE","SILVER","GOLD","PLATINUM","EMERALD","DIAMOND","MASTER","GRANDMASTER","CHALLENGER"]
DIV_ORDER = ["IV","III","II","I"]

@dataclass
class RankState:
    tier: Optional[str]
    division: Optional[str]
    lp: Optional[int]
    wins: Optional[int]
    losses: Optional[int]
    queue_type: Optional[str]

    @property
    def pretty(self) -> str:
        if not self.tier:
            return "Unranked"
        if self.tier in {"MASTER","GRANDMASTER","CHALLENGER"}:
            return f"{self.tier.title()} {self.lp or 0} LP"
        return f"{self.tier.title()} {self.division} — {self.lp or 0} LP"

    def rank_key(self) -> Tuple[int,int,int]:
        if not self.tier:
            return (-1,-1,-1)
        t = TIERS_ORDER.index(self.tier) if self.tier in TIERS_ORDER else -1
        d = DIV_ORDER.index(self.division) if self.division in DIV_ORDER else -1
        return (t,d,self.lp or 0)

def better_than(a: RankState, b: RankState) -> bool:
    return a.rank_key() > b.rank_key()

def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def ts_days_ago(days: int) -> int:
    return int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())

def rank_tuple_key(tier: Optional[str], div: Optional[str], lp: Optional[int]) -> Tuple[int,int,int]:
    ti = TIERS_ORDER.index(tier) if tier in TIERS_ORDER else -1
    di = DIV_ORDER.index(div) if div in DIV_ORDER else -1
    return (ti, di, lp or 0)

# Rank delta helpers for weekly digest
def rank_tuple_better(a: Tuple[Optional[str],Optional[str]], b: Tuple[Optional[str],Optional[str]]) -> bool:
    at, ad = a; bt, bd = b
    ai = TIERS_ORDER.index(at) if at in TIERS_ORDER else -1
    bi = TIERS_ORDER.index(bt) if bt in TIERS_ORDER else -1
    if ai != bi: return ai > bi
    adi = DIV_ORDER.index(ad) if ad in DIV_ORDER else -1
    bdi = DIV_ORDER.index(bd) if bd in DIV_ORDER else -1
    return adi > bdi

def rank_change_label(prev_tier: Optional[str], prev_div: Optional[str], cur_tier: Optional[str], cur_div: Optional[str]) -> str:
    if not prev_tier or not cur_tier:
        return "—"
    if prev_tier == cur_tier and prev_div == cur_div:
        return "No change"
    arrow = "↑" if rank_tuple_better((cur_tier, cur_div), (prev_tier, prev_div)) else "↓"
    prev_disp = f"{prev_tier.title()} {prev_div}" if prev_tier not in {"MASTER","GRANDMASTER","CHALLENGER"} else prev_tier.title()
    cur_disp  = f"{cur_tier.title()} {cur_div}" if cur_tier  not in {"MASTER","GRANDMASTER","CHALLENGER"} else cur_tier.title()
    return f"{arrow} {prev_disp} → {cur_disp}"

# ------------- Core bot -------------
class LoLBot(discord.Client):
    def __init__(self, db: sqlite3.Connection, riot: RiotHTTP, *, intents: discord.Intents):
        super().__init__(intents=intents)
        self.db = db
        self.riot = riot
        self.tree = app_commands.CommandTree(self)
        self.ddragon_map: Dict[int, str] = {}  # champId -> name

    async def setup_hook(self):
        # Slash sync
        if GUILD_ID:
            guild_obj = discord.Object(id=int(GUILD_ID))
            self.tree.copy_global_to(guild=guild_obj)
            await self.tree.sync(guild=guild_obj)
        else:
            await self.tree.sync()
        log.info("Slash commands synced.")

        # Warm up DDragon champ map
        try:
            self.ddragon_map = await self.riot.ddragon_champion_map()
            log.info("DDragon champ map loaded (%d champs).", len(self.ddragon_map))
        except Exception as e:
            log.warning("Could not load DDragon map: %s", e)

        # Start periodic tasks
        self.start_watcher.start()
        self.weekly_reporter.start()

    # ---------- DB ops ----------
    def add_or_get_player(self, game_name: str, tag_line: str, puuid: str, summoner_id: str, platform: str) -> int:
        with db_connect() as c:
            c.execute(
                """
                INSERT OR IGNORE INTO players (discord_id, game_name, tag_line, puuid, summoner_id, platform)
                VALUES (NULL, ?, ?, ?, ?, ?)
                """,
                (game_name, tag_line, puuid, summoner_id, platform)
            )
            row = c.execute(
                "SELECT id FROM players WHERE game_name=? AND tag_line=? AND platform=?",
                (game_name, tag_line, platform)
            ).fetchone()
            return int(row[0])

    def get_all_players(self) -> List[sqlite3.Row]:
        with db_connect() as c:
            return c.execute("SELECT * FROM players ORDER BY game_name").fetchall()

    def get_player(self, game_name: str, tag_line: str) -> Optional[Dict]:
        cur = self.db.cursor()
        cur.execute(
            "SELECT * FROM players WHERE game_name=? AND tag_line=?",
            (game_name.strip(), tag_line.strip()),
        )
        row = cur.fetchone()
        if not row:
            return None
        return dict(zip([c[0] for c in cur.description], row))

    def save_rank_snapshot(self, player_id: int, queue_type: str, rs: RankState):
        with db_connect() as c:
            c.execute(
                """
                INSERT INTO rank_snapshots (player_id, ts_utc, queue_type, tier, division, lp, wins, losses, games_ranked_solo)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    player_id,
                    now_ts(),
                    queue_type,
                    rs.tier,
                    rs.division,
                    rs.lp,
                    rs.wins,
                    rs.losses,
                )
            )
            c.commit()

    def last_snapshot_for_queue(self, player_id: int, queue_type: str) -> Optional[sqlite3.Row]:
        with db_connect() as c:
            return c.execute(
                "SELECT * FROM rank_snapshots WHERE player_id=? AND queue_type=? ORDER BY ts_utc DESC LIMIT 1",
                (player_id, queue_type)
            ).fetchone()

    def first_snapshot_since(self, player_id: int, queue_type: str, since_ts: int) -> Optional[sqlite3.Row]:
        with db_connect() as c:
            return c.execute(
                "SELECT * FROM rank_snapshots WHERE player_id=? AND queue_type=? AND ts_utc>=? ORDER BY ts_utc ASC LIMIT 1",
                (player_id, queue_type, since_ts)
            ).fetchone()

    # Weekly digest bookkeeping
    def has_weekly_posted(self, year_iso: int, week_iso: int) -> bool:
        with db_connect() as c:
            row = c.execute(
                "SELECT 1 FROM weekly_reports WHERE year_iso=? AND week_iso=?",
                (year_iso, week_iso)
            ).fetchone()
            return row is not None

    def mark_weekly_posted(self, year_iso: int, week_iso: int):
        with db_connect() as c:
            c.execute(
                "INSERT OR IGNORE INTO weekly_reports (week_iso, year_iso, posted_ts_utc) VALUES (?, ?, ?)",
                (week_iso, year_iso, now_ts())
            )
            c.commit()

    # ---------- Riot helpers ----------
    async def resolve_riot_id(self, game_name: str, tag_line: str) -> Optional[Dict]:
        for shard in ["americas", "europe", "asia"]:
            try:
                account = await self.riot.by_riot_id(shard, game_name, tag_line)
                if account and "puuid" in account:
                    return account
            except Exception:
                continue
        return None

    async def resolve_summoner_platform(self, puuid: str) -> Tuple[Optional[str], Optional[Dict]]:
        for plat in ALL_PLATFORMS:
            try:
                data = await self.riot.by_puuid_summoner_v4(puuid, plat)
                if isinstance(data, dict) and data and (data.get("puuid") == puuid or "profileIconId" in data):
                    return plat, data
            except Exception:
                continue
        return None, None

    async def fetch_rank_states(self, puuid: str, platform: str) -> Tuple[Optional[RankState], Optional[RankState]]:
        try:
            entries = await self.riot.league_entries_by_puuid(puuid, platform)
        except Exception as e:
            log.error(f"rank entries failed for {puuid}@{platform}: {e}")
            return None, None
        if not entries:
            return None, None

        def to_state(e: Optional[Dict]) -> Optional[RankState]:
            if not e:
                return None
            return RankState(
                tier=e.get("tier"),
                division=e.get("rank"),
                lp=e.get("leaguePoints"),
                wins=e.get("wins"),
                losses=e.get("losses"),
                queue_type=e.get("queueType"),
            )
        solo = next((to_state(e) for e in entries if e.get("queueType") == "RANKED_SOLO_5x5"), None)
        flex = next((to_state(e) for e in entries if e.get("queueType") == "RANKED_FLEX_SR"), None)
        return solo, flex

    async def compute_wr(self, puuid: str, platform: str, n: int = 20) -> Tuple[int, int, float]:
        regional = regional_from_platform(platform)
        ids = await self.riot.match_ids(puuid, regional, queue=420, count=n)
        if not ids:
            return 0, 0, 0.0
        wins = losses = 0
        for mid in ids:
            data = await self.riot.match(mid, regional)
            if not data:
                continue
            part = next((p for p in data["info"]["participants"] if p["puuid"] == puuid), None)
            if part and part.get("win"):
                wins += 1
            else:
                losses += 1
        total = wins + losses
        wr = (wins / total * 100.0) if total else 0.0
        return wins, losses, wr

    # ---------- Discord helpers ----------
    def make_rank_embed(self, title: str, solo: Optional[RankState], flex: Optional[RankState],
                        wr20: Optional[Tuple[int, int, float]] = None) -> discord.Embed:
        def line(rs: Optional[RankState]) -> str:
            if not rs or not rs.tier:
                return "Unranked"
            base = (f"{rs.tier.title()} {rs.division}" if rs.tier not in {"MASTER","GRANDMASTER","CHALLENGER"}
                    else rs.tier.title())
            lp_part = f"{rs.lp or 0} LP"
            wl_part = f"{(rs.wins or 0)}W {(rs.losses or 0)}L"
            return f"{base} — {lp_part} ({wl_part})"

        emb = discord.Embed(title=title, color=0x00A8FF)
        emb.add_field(name="Solo/Duo", value=line(solo), inline=False)
        emb.add_field(name="Flex SR", value=line(flex), inline=False)
        if wr20:
            w, l, wr = wr20
            emb.add_field(name="WR (last 20 solo)", value=f"**{w}W {l}L** — {wr:.1f}%", inline=False)
        emb.set_footer(text="Made by Nix - Rito API")
        return emb

    # ---------- LP delta utilities ----------
    def lp_delta_since(self, player_id: int, queue_type: str, since_ts: int) -> Optional[int]:
        """LP(now) - LP(first_snapshot_after_since). Returns None if insufficient data or unranked."""
        with db_connect() as c:
            last = c.execute(
                "SELECT lp FROM rank_snapshots WHERE player_id=? AND queue_type=? ORDER BY ts_utc DESC LIMIT 1",
                (player_id, queue_type)
            ).fetchone()
            first = c.execute(
                "SELECT lp, tier, division FROM rank_snapshots WHERE player_id=? AND queue_type=? AND ts_utc>=? ORDER BY ts_utc ASC LIMIT 1",
                (player_id, queue_type, since_ts)
            ).fetchone()
        if not last or not first:
            return None
        if last["lp"] is None or first["lp"] is None:
            return None
        return int(last["lp"]) - int(first["lp"])

    # ---------- Periodic tasks ----------
    @tasks.loop(minutes=WATCH_INTERVAL_MIN)
    async def start_watcher(self):
        await self.wait_until_ready()
        log.info("Watcher tick…")
        channel = None
        try:
            channel = self.get_channel(ALERT_CHANNEL_ID)
        except Exception:
            channel = None

        for p in self.get_all_players():
            plat = p["platform"]
            puuid = p["puuid"]
            solo, flex = await self.fetch_rank_states(puuid, plat)
            # Save snapshots if available
            if solo: self.save_rank_snapshot(p["id"], "RANKED_SOLO_5x5", solo)
            if flex: self.save_rank_snapshot(p["id"], "RANKED_FLEX_SR",  flex)

            if channel and solo:
                # Compare against previous solo snapshot within last 24h for quick pings
                prev = self.first_snapshot_since(p["id"], "RANKED_SOLO_5x5", ts_days_ago(1))
                last = self.last_snapshot_for_queue(p["id"], "RANKED_SOLO_5x5")
                if prev and last:
                    if (prev["tier"], prev["division"]) != (last["tier"], last["division"]):
                        arrow = "🔺" if rank_tuple_better((last["tier"], last["division"]), (prev["tier"], prev["division"])) else "🔻"
                        await channel.send(
                            f"{arrow} **{p['game_name']}#{p['tag_line']}** is now **{last['tier']} {last['division']}** ({plat.upper()})"
                        )
                    elif last["lp"] is not None and prev["lp"] is not None and last["lp"] != prev["lp"]:
                        delta = int(last["lp"]) - int(prev["lp"]) 
                        s = f"+{delta}" if delta >= 0 else f"{delta}"
                        await channel.send(
                            f"📈 **{p['game_name']}#{p['tag_line']}** LP change in last 24h: **{s} LP** ({plat.upper()})"
                        )

    @start_watcher.before_loop
    async def before_watcher(self):
        await self.wait_until_ready()
        log.info(f"Starting watcher every {WATCH_INTERVAL_MIN} minutes…")

    @tasks.loop(minutes=30)
    async def weekly_reporter(self):
        """Posts a weekly rank digest every WEEKLY_DAY @ WEEKLY_HOUR in WEEKLY_TZ."""
        await self.wait_until_ready()
        try:
            channel = self.get_channel(ALERT_CHANNEL_ID)
            if channel is None:
                return

            now_local = datetime.now(ZoneInfo(WEEKLY_TZ))
            if now_local.weekday() != WEEKLY_DAY or now_local.hour != WEEKLY_HOUR:
                return

            year_iso = now_local.isocalendar().year
            week_iso = now_local.isocalendar().week
            if self.has_weekly_posted(year_iso, week_iso):
                return  # already posted this week

            rows = self.get_all_players()
            if not rows:
                await channel.send("📣 Weekly LoL Update: no tracked players yet.")
                self.mark_weekly_posted(year_iso, week_iso)
                return

            lines: List[str] = []
            inactive: List[str] = []

            # Sort by current Solo rank
            scored = []
            for r in rows:
                solo, _ = await self.fetch_rank_states(r["puuid"], r["platform"])
                scored.append((rank_tuple_key(solo.tier if solo else None,
                                              solo.division if solo else None,
                                              solo.lp if solo else 0), r, solo))
            scored.sort(reverse=True, key=lambda x: x[0])

            since_ts = ts_days_ago(7)
            for _, r, solo in scored:
                # LP delta
                dlt = self.lp_delta_since(r["id"], "RANKED_SOLO_5x5", since_ts)
                dlt_str = f"{'+' if (dlt or 0) >= 0 else ''}{dlt} LP" if dlt is not None else "—"

                # Rank change label
                first = self.first_snapshot_since(r["id"], "RANKED_SOLO_5x5", since_ts)
                if first and solo:
                    change = rank_change_label(first["tier"], first["division"], solo.tier, solo.division)
                else:
                    change = "—"

                # Inactive (no snapshots at all in last 7d)
                with db_connect() as c:
                    act = c.execute(
                        "SELECT 1 FROM rank_snapshots WHERE player_id=? AND queue_type='RANKED_SOLO_5x5' AND ts_utc>=? LIMIT 1",
                        (r["id"], since_ts)
                    ).fetchone()
                if act is None:
                    inactive.append(f"{r['game_name']}#{r['tag_line']}")

                if not solo or not solo.tier:
                    lines.append(f"• **{r['game_name']}#{r['tag_line']}** — Unranked  |  7 days: {dlt_str}  |  Change: {change}")
                else:
                    div = solo.division if solo.tier not in {"MASTER","GRANDMASTER","CHALLENGER"} else ""
                    lines.append(
                        f"• **{r['game_name']}#{r['tag_line']}** — {solo.tier.title()} {div} • {solo.lp} LP  |  7 days: {dlt_str}  |  Change: {change}"
                    )

            title = f"📣 Weekly LoL Update — Week {year_iso}-W{week_iso:02d}"
            desc = "\n".join(lines) if lines else "No ranks available."

            emb = discord.Embed(title=title, description=desc, color=0x1ABC9C)
            if inactive:
                emb.add_field(name="No ranked Solo games in last 7 days", value=", ".join(inactive), inline=False)
            emb.set_footer(text=f"{WEEKLY_TZ} • Made by Nix - Snapshot from last week.")

            await channel.send(embed=emb)
            self.mark_weekly_posted(year_iso, week_iso)
        except Exception as e:
            log.exception("weekly_reporter failed: %s", e)

# ------------- App helpers -------------
async def build_profile_embed(bot: LoLBot, player_row: sqlite3.Row) -> discord.Embed:
    puuid = player_row["puuid"]
    plat = player_row["platform"]
    summ = await bot.riot.by_puuid_summoner_v4(puuid, plat) or {}
    level = summ.get("summonerLevel")
    icon = summ.get("profileIconId")
    icon_url = f"https://ddragon.leagueoflegends.com/cdn/14.10.1/img/profileicon/{icon}.png" if icon is not None else "N/A"

    total_score = await bot.riot.mastery_score(puuid, plat)
    tops = await bot.riot.mastery_top(puuid, plat, count=3) or []

    def champ_name(cid: int) -> str:
        return bot.ddragon_map.get(cid, f"#{cid}")

    top_lines = []
    for m in tops:
        cid = int(m.get("championId", -1))
        pts = m.get("championPoints", 0)
        lvl = m.get("championLevel", 0)
        top_lines.append(f"• {champ_name(cid)} — {pts} pts (Lv{lvl})")

    emb = discord.Embed(
        title=f"{player_row['game_name']}#{player_row['tag_line']} — Profile",
        color=0x3498DB,
        description=f"Level **{level}**\nIcon: {icon}  \n{icon_url}"
    )
    emb.add_field(name="Total Mastery Score", value=str(total_score), inline=False)
    emb.add_field(name="Top Mastery Champs", value=("\n".join(top_lines) if top_lines else "—"), inline=False)
    emb.set_footer(text=f"{plat.upper()} • PUUID only (privacy-safe)")
    return emb

async def make_leaderboard(bot: LoLBot, limit: int = 10) -> discord.Embed:
    rows = bot.get_all_players()
    scored: List[Tuple[Tuple[int,int,int], sqlite3.Row, Optional[RankState]]] = []
    for r in rows:
        solo, _ = await bot.fetch_rank_states(r["puuid"], r["platform"])
        scored.append((rank_tuple_key(solo.tier if solo else None,
                                      solo.division if solo else None,
                                      solo.lp if solo else 0), r, solo))
    scored.sort(reverse=True, key=lambda x: x[0])

    desc_lines = []
    for i, (_, r, solo) in enumerate(scored[:limit], 1):
        if not solo or not solo.tier:
            desc_lines.append(f"{i}. **{r['game_name']}#{r['tag_line']}** — Unranked")
        else:
            div = solo.division if solo.tier not in {"MASTER","GRANDMASTER","CHALLENGER"} else ""
            desc_lines.append(
                f"{i}. **{r['game_name']}#{r['tag_line']}** — {solo.tier.title()} {div} • {solo.lp} LP"
            )
    emb = discord.Embed(title=f"Server Leaderboard (Top {limit})", color=0xF1C40F, description="\n".join(desc_lines) or "No players.")
    emb.set_footer(text="Ranked Solo • Made by Nix")
    return emb

async def make_climb_race(bot: LoLBot, days: int = 7, limit: int = 10) -> discord.Embed:
    since_ts = ts_days_ago(days)
    rows = bot.get_all_players()
    deltas: List[Tuple[int, sqlite3.Row]] = []
    for r in rows:
        delta = bot.lp_delta_since(r["id"], "RANKED_SOLO_5x5", since_ts)
        if delta is not None:
            deltas.append((delta, r))
    deltas.sort(reverse=True, key=lambda x: x[0])

    lines = []
    for i, (dlt, r) in enumerate(deltas[:limit], 1):
        sign = f"+{dlt}" if dlt >= 0 else str(dlt)
        lines.append(f"{i}. **{r['game_name']}#{r['tag_line']}** — {sign} LP (last {days}d)")
    if not lines:
        lines = ["No LP changes recorded in that period."]

    emb = discord.Embed(title=f"Climb Race — last {days} days", color=0x2ECC71, description="\n".join(lines))
    emb.set_footer(text="Based on rank snapshots stored by the watcher")
    return emb

async def make_compare(bot: LoLBot, a_row: sqlite3.Row, b_row: sqlite3.Row) -> discord.Embed:
    a_solo, _ = await bot.fetch_rank_states(a_row["puuid"], a_row["platform"])
    b_solo, _ = await bot.fetch_rank_states(b_row["puuid"], b_row["platform"])
    def fmt(r: sqlite3.Row, s: Optional[RankState]) -> str:
        if not s or not s.tier:
            return f"**{r['game_name']}#{r['tag_line']}** — Unranked"
        div = s.division if s.tier not in {"MASTER","GRANDMASTER","CHALLENGER"} else ""
        return f"**{r['game_name']}#{r['tag_line']}** — {s.tier.title()} {div} • {s.lp} LP"
    winner = None
    if a_solo and b_solo and a_solo.tier and b_solo.tier:
        winner = "A" if better_than(a_solo, b_solo) else ("B" if better_than(b_solo, a_solo) else "Tie")
    emb = discord.Embed(title="Compare Ranks (Solo/Duo)", color=0x9B59B6)
    emb.add_field(name="Player A", value=fmt(a_row, a_solo), inline=False)
    emb.add_field(name="Player B", value=fmt(b_row, b_solo), inline=False)
    if winner:
        t = {"A":"Player A leads 🏆","B":"Player B leads 🏆","Tie":"It’s a tie 🤝"}[winner]
        emb.set_footer(text=t)
    return emb

# ------------- Main -------------
if __name__ == "__main__":
    if not DISCORD_TOKEN:
        log.error("Missing DISCORD_TOKEN in env.")
        raise SystemExit(1)
    if not RIOT_API_KEY:
        log.warning("RIOT_API_KEY missing at startup; set it in bot.env and restart the service.")

    async def main():
        db = db_connect()
        http_session = aiohttp.ClientSession()
        riot = RiotHTTP(http_session)

        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.members = True

        bot = LoLBot(db=db, riot=riot, intents=intents)

        # ---------- Commands ----------
        # ---------- Admin Controls ----------
        from discord.app_commands import check as _check

        def is_admin():
            async def predicate(interaction: discord.Interaction) -> bool:
                return interaction.user.guild_permissions.administrator
            return _check(predicate)

        
        
        @bot.tree.command(name="admin_restart", description="(Admin) Restart the bot process")
        @is_admin()
        async def admin_restart(interaction: discord.Interaction):
            await interaction.response.send_message("♻️ Restarting…", ephemeral=True)
            # let the response flush, then close; systemd (if used) will restart it
            async def _delayed_close():
                await asyncio.sleep(0.5)
                await bot.close()
            asyncio.create_task(_delayed_close())

        from discord import app_commands

        @bot.tree.command(name="lol_add", description="Add one or more Riot IDs (comma or newline separated)")
        @app_commands.describe(riot_ids="Example: Name#TAG, Other#EUW, Third#NA1")
        @app_commands.guild_only()
        async def lol_add(interaction: discord.Interaction, riot_ids: str):
            await interaction.response.defer(thinking=True, ephemeral=True)
            try:
                items = [s.strip() for s in riot_ids.replace("\n", ",").split(",") if s.strip()]
                if not items:
                    await interaction.followup.send("Provide at least one Riot ID (Name#TAG).")
                    return

                import re
                pat = re.compile(r"^.{3,32}#[A-Za-z0-9]{2,5}$")

                results = []
                for item in items:
                    if not pat.match(item):
                        results.append(f"⚠️ {item} — invalid format (use Name#TAG)")
                        continue
                    game_name, tag_line = item.split("#", 1)
                    try:
                        msg = await bot._add_one_riot_id(game_name, tag_line)
                    except Exception as ie:
                        msg = f"❌ {item} — {str(ie)[:120]}"
                    results.append(msg)
                    await asyncio.sleep(0.2)

                await interaction.followup.send("\n".join(results) if results else "(nothing processed)")
            except Exception as e:
                log.exception("/lol_add failed")
                await interaction.followup.send(f"Error: {e}")

        @bot.tree.command(name="lol_add_many", description="Add multiple Riot IDs at once (comma or newline separated)")
        @app_commands.describe(riot_ids="List: Name#TAG, Name2#EUW, ...")
        @app_commands.guild_only()
        async def lol_add_many(interaction: discord.Interaction, riot_ids: str):
            await lol_add(interaction, riot_ids)  # type: ignore


        @app_commands.guild_only()
        @bot.tree.command(name="lol_rank", description="Show current Solo & Flex rank (with LP/WL)")
        @app_commands.describe(riot_id="GameName#TAG")
        async def lol_rank(interaction: discord.Interaction, riot_id: str):
            await interaction.response.defer()
            try:
                if "#" not in riot_id:
                    await interaction.followup.send("Invalid format. Use GameName#TAG.")
                    return
                game_name, tag_line = riot_id.split("#", 1)
                row = bot.get_player(game_name, tag_line)
                if not row:
                    await interaction.followup.send("Player not found. Use `/lol_add` first.")
                    return
                plat, _ = await bot.resolve_summoner_platform(row["puuid"])
                if plat and plat != row["platform"]:
                    with db_connect() as c:
                        c.execute("UPDATE players SET platform=? WHERE id=?", (plat, row["id"]))
                        c.commit()
                    row["platform"] = plat
                solo, flex = await bot.fetch_rank_states(row["puuid"], row["platform"])
                wr20 = await bot.compute_wr(row["puuid"], row["platform"], n=20)
                title = f"{row['game_name']}#{row['tag_line']} — Current Ranks ({row['platform'].upper()})"
                await interaction.followup.send(embed=bot.make_rank_embed(title, solo, flex, wr20=wr20))
            except Exception as e:
                log.exception("/lol_rank failed")
                await interaction.followup.send(f"Unexpected error: {e}")

        @app_commands.guild_only()
        @bot.tree.command(name="lol_profile", description="Show level, profile icon, and top mastery champs")
        @app_commands.describe(riot_id="GameName#TAG")
        async def lol_profile(interaction: discord.Interaction, riot_id: str):
            await interaction.response.defer()
            if "#" not in riot_id:
                await interaction.followup.send("Invalid format. Use GameName#TAG.")
                return
            game_name, tag_line = riot_id.split("#", 1)
            row = bot.get_player(game_name, tag_line)
            if not row:
                await interaction.followup.send("Player not found. Use `/lol_add` first.")
                return
            emb = await build_profile_embed(bot, row)
            await interaction.followup.send(embed=emb)

        @app_commands.guild_only()
        @bot.tree.command(name="lol_progress", description="LP delta since the last N days (Solo/Duo)")
        @app_commands.describe(riot_id="GameName#TAG", days="Window in days (default 7)")
        async def lol_progress(interaction: discord.Interaction, riot_id: str, days: app_commands.Range[int, 1, 60] = 7):
            await interaction.response.defer()
            if "#" not in riot_id:
                await interaction.followup.send("Invalid format. Use GameName#TAG.")
                return
            game_name, tag_line = riot_id.split("#", 1)
            row = bot.get_player(game_name, tag_line)
            if not row:
                await interaction.followup.send("Player not found. Use `/lol_add` first.")
                return
            dlt = bot.lp_delta_since(row["id"], "RANKED_SOLO_5x5", ts_days_ago(days))
            if dlt is None:
                await interaction.followup.send(f"No LP data for **{game_name}#{tag_line}** in last {days} days.")
                return
            sign = f"+{dlt}" if dlt >= 0 else str(dlt)
            await interaction.followup.send(f"**{game_name}#{tag_line}** LP change over last {days} days: **{sign} LP**")

        @app_commands.guild_only()
        @bot.tree.command(name="lol_leaderboard", description="Top N players by Solo rank/LP")
        @app_commands.describe(limit="How many to show (default 10)")
        async def lol_leaderboard(interaction: discord.Interaction, limit: app_commands.Range[int, 1, 25] = 10):
            await interaction.response.defer()
            emb = await make_leaderboard(bot, limit=limit)
            await interaction.followup.send(embed=emb)

        @app_commands.guild_only()
        @bot.tree.command(name="lol_climb_race", description="Biggest LP gainers over the last N days")
        @app_commands.describe(days="Window (default 7)", limit="How many to show (default 10)")
        async def lol_climb_race(interaction: discord.Interaction,
                                 days: app_commands.Range[int, 1, 60] = 7,
                                 limit: app_commands.Range[int, 1, 25] = 10):
            await interaction.response.defer()
            emb = await make_climb_race(bot, days=days, limit=limit)
            await interaction.followup.send(embed=emb)

        @app_commands.guild_only()
        @bot.tree.command(name="lol_compare", description="Compare two players by Solo rank")
        @app_commands.describe(riot_id_a="GameName#TAG (Player A)", riot_id_b="GameName#TAG (Player B)")
        async def lol_compare(interaction: discord.Interaction, riot_id_a: str, riot_id_b: str):
            await interaction.response.defer()
            if "#" not in riot_id_a or "#" not in riot_id_b:
                await interaction.followup.send("Use GameName#TAG for both players.")
                return
            a_gn, a_tl = riot_id_a.split("#", 1)
            b_gn, b_tl = riot_id_b.split("#", 1)
            a_row = bot.get_player(a_gn, a_tl); b_row = bot.get_player(b_gn, b_tl)
            if not a_row or not b_row:
                await interaction.followup.send("Both players must be added first with `/lol_add`.")
                return
            emb = await make_compare(bot, a_row, b_row)
            await interaction.followup.send(embed=emb)

        # Admin tester: post digest now (ignores weekly lock)
        @app_commands.guild_only()
        @bot.tree.command(name="lol_weekly_now", description="Post the weekly digest right now (admin)")
        async def lol_weekly_now(interaction: discord.Interaction):
            await interaction.response.defer()
            try:
                channel = bot.get_channel(ALERT_CHANNEL_ID)
                if channel is None:
                    await interaction.followup.send("Configured channel not found.")
                    return

                rows = bot.get_all_players()
                if not rows:
                    await interaction.followup.send("No tracked players.")
                    return

                lines = []
                inactive = []
                scored = []
                for r in rows:
                    solo, _ = await bot.fetch_rank_states(r["puuid"], r["platform"])
                    scored.append((rank_tuple_key(solo.tier if solo else None,
                                                  solo.division if solo else None,
                                                  solo.lp if solo else 0), r, solo))
                scored.sort(reverse=True, key=lambda x: x[0])

                since_ts = ts_days_ago(7)
                for _, r, solo in scored:
                    dlt = bot.lp_delta_since(r["id"], "RANKED_SOLO_5x5", since_ts)
                    dlt_str = f"{'+' if (dlt or 0) >= 0 else ''}{dlt} LP" if dlt is not None else "—"
                    first = bot.first_snapshot_since(r["id"], "RANKED_SOLO_5x5", since_ts)
                    if first and solo:
                        change = rank_change_label(first["tier"], first["division"], solo.tier, solo.division)
                    else:
                        change = "—"
                    with db_connect() as c:
                        act = c.execute(
                            "SELECT 1 FROM rank_snapshots WHERE player_id=? AND queue_type='RANKED_SOLO_5x5' AND ts_utc>=? LIMIT 1",
                            (r["id"], since_ts)
                        ).fetchone()
                    if act is None:
                        inactive.append(f"{r['game_name']}#{r['tag_line']}")

                    if not solo or not solo.tier:
                        lines.append(f"• **{r['game_name']}#{r['tag_line']}** — Unranked  |  7 days: {dlt_str}  |  Change: {change}")
                    else:
                        div = solo.division if solo.tier not in {"MASTER","GRANDMASTER","CHALLENGER"} else ""
                        lines.append(f"• **{r['game_name']}#{r['tag_line']}** — {solo.tier.title()} {div} • {solo.lp} LP  |  7 days: {dlt_str}  |  Change: {change}")

                now_local = datetime.now(ZoneInfo(WEEKLY_TZ))
                y = now_local.isocalendar().year
                w = now_local.isocalendar().week
                title = f"📣 Weekly LoL Update — Week {y}-W{w:02d}"
                emb = discord.Embed(title=title, description="\n".join(lines) or "No ranks.", color=0x1ABC9C)
                if inactive:
                    emb.add_field(name="No ranked Solo games in last 7 days", value=", ".join(inactive), inline=False)
                emb.set_footer(text=f"{WEEKLY_TZ} • Made by Nix - Snapshot from the last week.")

                await channel.send(embed=emb)
                await interaction.followup.send("Posted ✅")
            except Exception as e:
                await interaction.followup.send(f"Error: {e}")

        await bot.start(DISCORD_TOKEN)

    asyncio.run(main())
