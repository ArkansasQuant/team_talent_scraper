"""
Scrape active rosters for all FBS teams across seasons from 247sports,
then enrich each roster row by visiting the player profile to capture
transfer-portal metadata (origin team, rating, ranks, transfer class year).

Design principles (from scraping playbook):
  - Playwright + domcontentloaded (NOT networkidle)
  - Randomized delays + user-agent rotation
  - NEVER break-on-exception in loops — use continue + failure counter
  - Per-team-season checkpointing for crash recovery
  - GLOBAL profile cache keyed by 247_id — same player across N seasons
    is scraped exactly ONCE per workflow run (massive time saver)
  - Cache flushed to disk after every team-season so a mid-run crash
    can resume without re-scraping any profiles
  - Profile fetches run concurrently (asyncio.Semaphore) to keep total
    runtime under the 6hr GitHub Actions limit
  - Validate row counts against expected per-team norms
  - Post-run verification (row counts, ID format, null-ratio sanity)

Usage:
  # Single season, all teams, with profile enrichment (default):
  python scrape_rosters.py --seasons 2024 --output roster_2024.csv

  # Multiple seasons:
  python scrape_rosters.py --seasons 2018 2019 2020 --output roster.csv

  # Test a couple of teams:
  python scrape_rosters.py --teams Troy Alabama --seasons 2022

  # Roster-only (skip profile enrichment, much faster):
  python scrape_rosters.py --seasons 2024 --skip-profiles --output roster.csv
"""
import argparse
import asyncio
import csv
import json
import os
import random
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from team_urls import TEAM_URLS, team_url, is_fbs_in_year, all_teams

# ---------- Config ----------
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

DELAY_MIN = 1.0   # was 3.0 — between-team-season delay
DELAY_MAX = 3.0   # was 8.0
NAV_TIMEOUT_MS = 30000
PROFILE_NAV_TIMEOUT_MS = 30000
PROFILE_SELECTOR_TIMEOUT_MS = 5000    # short — we tolerate timeout anyway,
                                      # so don't burn 15s waiting on profiles
                                      # that just don't have a rankings section
MAX_CONSECUTIVE_FAILURES = 8     # abort if this many in a row fail
MAX_RETRIES_PER_PAGE = 3
MAX_PROFILE_RETRIES = 2          # profile fetches: 1 retry, not 2
DEFAULT_PROFILE_CONCURRENCY = 6  # was 4 — 247 tolerates this fine
PROFILE_DELAY_MIN = 0.3          # was 0.8 — with concurrency=6, this is
PROFILE_DELAY_MAX = 1.0          # ~6 fetches/sec, well under any rate limit

# Bump this when the cache schema changes so old caches get thrown out.
# v3 added prospect_event (HS recruiting / JUCO data) + national_rank field.
PROFILE_CACHE_SCHEMA = 3

CHECKPOINT_DIR = Path("checkpoints")
PROFILE_CACHE_FILE = CHECKPOINT_DIR / "profiles_cache.json"

# Regex to extract 247 ID from player URL, e.g. /player/carlton-martial-91227/
PLAYER_URL_RE = re.compile(r'/player/([^/]*?)-(\d+)/?$')

# Columns added by the profile enrichment step
TRANSFER_FIELDS = [
    # ---- Transfer portal data (only populated if player went through portal) ----
    'transfer_origin_team',       # team they came from (pre-roster)
    'transfer_destination_team',  # team they committed to (per profile commit banner)
    'transfer_rating',            # numeric 247 transfer rating, e.g. "88"
    'transfer_overall_rank',      # transfer portal overall rank
    'transfer_position_rank',     # transfer portal position rank
    'transfer_position',          # position label associated with rank (QB, WR, etc.)
    'transfer_class_year',        # transfer class year, e.g. "2025"
    'transfer_stars',             # 1-5 star count from transfer section, or 'JUCO'

    # ---- HS recruiting / JUCO data (populated independently of transfer) ----
    'hs_class_year',              # the (YYYY) from the prospect rank-block
    'hs_rating',                  # numeric 247Sports composite rating
    'hs_national_rank',           # national rank (only top ~500 ranked)
    'hs_position_rank',           # position rank within class
    'hs_position',                # position label (QB, WR, etc.)
    'hs_stars',                   # 1-5 stars or 'JUCO'
    'hs_section_kind',            # '247Sports' or 'JUCO' (helps consumer distinguish)

    # ---- Diagnostic ----
    # profile_scraped is one of:
    #   'ok'              — fetched + parsed + matching transfer event applied
    #   'ok_no_transfer'  — fetched + parsed but no transfer event applies for
    #                       this season (player never transferred OR transfer
    #                       is in the future). HS fields may still be populated.
    #   'failed'          — fetch error after all retries
    #   'no_url'          — row had no profile URL (placeholder/walk-on)
    #   'skipped'         — enrichment was disabled via --skip-profiles
    'profile_scraped',
]

ALL_OUTPUT_COLS = [
    '247_id', 'player_name', 'team', 'season', 'jersey', 'position',
    'height', 'weight', 'class_yr', 'age', 'high_school', 'rating_247',
    'profile_url', 'scrape_ts',
] + TRANSFER_FIELDS


# ---------- Roster page extraction ----------
def parse_roster_html(html, team, season):
    """
    Extract roster rows from a team roster page.

    Returns a list of dicts, one per roster entry. Combines the player-link
    list (which has names + 247 IDs) with the jersey table (which has
    position, height, weight, class year, HS, rating) by row order.

    247 returns the roster as TWO parallel structures:
      1. A <ul> or <div> of player name links (w/ 247 IDs in URLs)
      2. A <table> with jersey/position/height/weight/yr/age/HS/rating
    These are aligned row-by-row in source order.
    """
    soup = BeautifulSoup(html, 'lxml')
    rows = []

    # 1. Find all player links on the page
    player_anchors = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        m = PLAYER_URL_RE.search(href)
        if m:
            slug, pid = m.group(1), m.group(2)
            # Detect placeholder/empty-anchor entries
            is_placeholder = (
                pid == '46120272'
                or slug == ''
                or set(slug) <= {'-'}
            )
            name = a.get_text(strip=True)
            if name:
                # IMPORTANT: 247 now returns absolute hrefs (https://247sports.com/...)
                # in addition to relative ones, so detect and avoid double-prefixing.
                # Pre-2026 we always prepended the host; that bug produced URLs like
                # https://247sports.comhttps://247sports.com/player/... and every
                # profile fetch died with ERR_NAME_NOT_RESOLVED.
                if href.startswith('http://') or href.startswith('https://'):
                    full_url = href.rstrip('/') + '/'
                else:
                    full_url = f"https://247sports.com{href.rstrip('/')}/"
                player_anchors.append({
                    'name': name,
                    '247_id': None if is_placeholder else pid,
                    'profile_url': None if is_placeholder else full_url,
                })

    # 2. Find roster data rows
    data_rows = []
    for table in soup.find_all('table'):
        trs = table.find_all('tr')
        if len(trs) < 2:
            continue
        header_cells = [c.get_text(strip=True).lower() for c in trs[0].find_all(['th', 'td'])]
        header_text = ' '.join(header_cells)
        if not ('jersey' in header_text or 'pos' in header_text or 'height' in header_text):
            continue
        for tr in trs[1:]:
            tds = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
            if len(tds) < 6:
                continue
            data_rows.append(tds)
        break  # first matching table

    if not data_rows:
        return []

    # 3. Find the longest contiguous window of player anchors equal to the
    # number of data rows. Score = count of non-null 247_ids in window.
    n = len(data_rows)
    best_window = None
    for i in range(len(player_anchors) - n + 1):
        window = player_anchors[i:i + n]
        score = sum(1 for a in window if a['247_id'])
        if best_window is None or score > best_window['score']:
            best_window = {'anchors': window, 'score': score, 'start': i}

    if not best_window or best_window['score'] == 0:
        anchors = player_anchors[:n] if len(player_anchors) >= n else None
        if anchors is None:
            return []
    else:
        anchors = best_window['anchors']

    # 4. Zip anchors with data rows
    for anchor, data in zip(anchors, data_rows):
        # Data table cols: Jersey | POS | Height | Weight | Yr | Age | HS | Rating
        data = (data + [''] * 8)[:8]
        jersey, pos, height, weight, yr, age, hs, rating = data
        # Convert "6-2" / "5-10" → 6'2" / 5'10" so Excel/pandas don't auto-date
        # them to "2-Jun" / "10-May". 247 always serves height as "FT-IN".
        m_h = re.match(r'^\s*(\d{1,2})-(\d{1,2})\s*$', height or '')
        if m_h:
            height = f"{m_h.group(1)}'{m_h.group(2)}\""
        row = {
            '247_id': anchor['247_id'],
            'player_name': anchor['name'],
            'team': team,
            'season': season,
            'jersey': jersey,
            'position': pos,
            'height': height,
            'weight': weight,
            'class_yr': yr,
            'age': age,
            'high_school': hs,
            'rating_247': rating if rating.lower() != 'na' else '',
            'profile_url': anchor['profile_url'] or '',
            'scrape_ts': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
        }
        # Initialize transfer fields as empty (filled in by enrichment step)
        for f in TRANSFER_FIELDS:
            row[f] = ''
        rows.append(row)
    return rows


# ---------- Player profile extraction (transfer subsection) ----------
def _parse_one_section(section, is_juco_title):
    """Parse a single rankings-section into a normalized event dict.

    Returns dict with: kind ('Transfer'|'247Sports'|'JUCO'),
    rating, year (int|None), overall_rank, national_rank, position_rank,
    position, stars.
    """
    out = {
        'kind': '', 'rating': '', 'year': None,
        'overall_rank': '', 'national_rank': '',
        'position_rank': '', 'position': '', 'stars': '',
    }

    # Rating + year live in .rank-block. Anchor regex to START of stripped text
    # — the rank-block can prefix badges or other digits; we want only the
    # leading numeric rating value. (Per playbook rule #3.)
    rating_block = section.select_one('.rank-block')
    if rating_block:
        rating_text = rating_block.get_text(' ', strip=True)
        m_rating = re.match(r'^\s*(\d+(?:\.\d+)?)', rating_text)
        if m_rating:
            out['rating'] = m_rating.group(1)
        m_year = re.search(r'\((\d{4})\)', rating_text)
        if m_year:
            out['year'] = int(m_year.group(1))

    # Stars — JUCO sections don't render gold stars, mark as 'JUCO'.
    if is_juco_title:
        out['stars'] = 'JUCO'
    else:
        stars = section.select('span.icon-starsolid.yellow')
        if stars:
            out['stars'] = str(min(len(stars), 5))

    # Ranks: <li><b>LABEL</b><strong>VALUE</strong></li>
    # Use href to disambiguate position rank from state rank.
    for li in section.select('li'):
        bold = li.find('b')
        strong = li.find('strong')
        if not bold or not strong:
            continue
        label = bold.get_text(strip=True).upper()
        value = strong.get_text(strip=True)
        link = li.find('a')
        href = (link.get('href', '') if link else '')

        if 'OVR' in label or 'OVERALL' in label:
            out['overall_rank'] = value
        elif 'NATL' in label or 'NATIONAL' in label:
            # Always capture national rank into its own field. Harmless for
            # transfer sections (they don't have NATL anyway); essential for
            # prospect (HS recruiting) sections — those use NATL not OVR.
            out['national_rank'] = value
        elif 'State=' in href:
            # State rank — not what we want
            continue
        elif not out['position_rank']:
            # First non-OVR/non-NATL/non-state rank => position rank
            out['position_rank'] = value
            out['position'] = label
    return out


def parse_player_profile(html):
    """
    Pull ALL transfer events + the prospect (HS/JUCO) event +
    origin/destination teams from a profile.

    Returns:
      {
        'origin_team': str,           # .team-info-section header h2
        'destination_team': str,      # .commit-banner span
        'transfer_events': [          # list, ordered as found on page
          {'kind':'Transfer','rating':'88','year':2024,
           'overall_rank':'123','national_rank':'',
           'position_rank':'5','position':'WR','stars':'4'}, ...
        ],
        'prospect_event': {           # ONE event — HS recruiting / JUCO
          'kind':'247Sports'|'JUCO', 'rating':'94','year':2022,
          'overall_rank':'','national_rank':'89',
          'position_rank':'5','position':'WR','stars':'4',
        } | None,
        'section_titles': [...],      # for diagnostics
      }

    Why a list of transfer events but only ONE prospect event? A player
    can transfer multiple times (multiple Transfer sections) but they
    only have ONE high-school recruiting class. Per playbook rule #1:
    NEVER `break` after the first Transfer section.

    Tie-break for prospect: if both 247Sports AND JUCO sections exist on
    the same profile (rare), prefer JUCO — it's the more specific data.
    Otherwise, take the first one we encounter.
    """
    soup = BeautifulSoup(html, 'lxml')
    result = {
        'origin_team': '',
        'destination_team': '',
        'transfer_events': [],
        'prospect_event': None,
        'section_titles': [],
    }

    # Origin team — .team-info-section header h2 (NOT .team-block; that
    # selector matches the same element and causes origin==destination bug)
    team_header = soup.select_one('.team-info-section header h2')
    if team_header:
        result['origin_team'] = team_header.get_text(strip=True)

    # Destination team — commit banner. Filter the literal label "Commit".
    commit_banner = soup.select_one('.commit-banner span')
    if commit_banner:
        txt = commit_banner.get_text(strip=True)
        if txt and txt.lower() != 'commit':
            result['destination_team'] = txt

    # Iterate ALL rankings sections, classify by title content (rule #2).
    for section in soup.select('section.rankings-section'):
        title_tag = section.select_one('h3.title')
        if not title_tag:
            continue
        title = title_tag.get_text(strip=True)
        result['section_titles'].append(title)

        is_juco = 'JUCO' in title
        if 'Transfer' in title:
            ev = _parse_one_section(section, is_juco_title=False)
            ev['kind'] = 'Transfer'
            result['transfer_events'].append(ev)
        elif title == '247Sports' or is_juco:
            ev = _parse_one_section(section, is_juco_title=is_juco)
            ev['kind'] = 'JUCO' if is_juco else '247Sports'
            # Tie-break: prefer JUCO over 247Sports if both appear; otherwise
            # keep the first prospect section we found.
            existing = result['prospect_event']
            if existing is None:
                result['prospect_event'] = ev
            elif is_juco and existing.get('kind') != 'JUCO':
                # JUCO found later — upgrade from 247Sports
                result['prospect_event'] = ev
            # else: keep existing (already JUCO, or first 247Sports wins)

    return result


def pick_transfer_event(profile, season):
    """Choose the transfer event that matches the given roster season.

    Priority order:
      1. Most recent dated event whose year is <= season
      2. Undated event (rank-block didn't yield a clean (YYYY)) — use the
         LAST one (most recent on page), since silently dropping it leaves
         all transfer fields blank and looks like a parser miss
      3. None — only when every event is dated AND in the future

    Why the undated fallback? Older 247 profiles sometimes lack the (YYYY)
    in the rank-block. The previous logic dropped these entirely, which
    was a SILENT failure mode — the profile fetched fine, the Transfer
    section parsed fine, we just didn't get a year and lost everything.
    """
    events = profile.get('transfer_events') or []
    if not events:
        return None

    # 1. Prefer dated events at-or-before this season; most recent year wins
    dated = [e for e in events if e.get('year') and e['year'] <= season]
    if dated:
        dated.sort(key=lambda e: e.get('year') or 0)
        return dated[-1]

    # 2. Fallback: events with no year at all — use the LAST (page-order)
    undated = [e for e in events if not e.get('year')]
    if undated:
        return undated[-1]

    # 3. All events are dated and in the future — don't pollute the row
    return None


# ---------- Profile cache (JSON, schema-versioned) ----------
def load_profile_cache():
    """Load cached profile lookups (full profile dicts keyed by 247_id).

    Discards the cache if:
      - schema version doesn't match (handles refactors like v1 → v2)
      - >50% of entries are 'failed' (poisoned by a bug)
    """
    if not PROFILE_CACHE_FILE.exists():
        return {}
    try:
        with open(PROFILE_CACHE_FILE, 'r', encoding='utf-8') as f:
            blob = json.load(f)
        if blob.get('schema') != PROFILE_CACHE_SCHEMA:
            print(f"  Profile cache schema mismatch "
                  f"(file={blob.get('schema')!r} vs code={PROFILE_CACHE_SCHEMA}); "
                  f"discarding & rebuilding.")
            return {}
        cache = blob.get('profiles', {}) or {}
        if cache:
            failed = sum(1 for v in cache.values() if v.get('fetch_status') == 'failed')
            if failed / len(cache) > 0.5:
                print(f"  Profile cache has {failed/len(cache):.0%} failures "
                      f"({failed}/{len(cache)}) — discarding as poisoned.")
                return {}
        print(f"  Loaded {len(cache):,} cached profiles from {PROFILE_CACHE_FILE}")
        return cache
    except Exception as e:
        print(f"  WARN: could not read profile cache ({e}); starting empty")
        return {}


def flush_profile_cache(cache):
    """Persist the profile cache as JSON (atomic write)."""
    if not cache:
        return
    PROFILE_CACHE_FILE.parent.mkdir(exist_ok=True)
    blob = {'schema': PROFILE_CACHE_SCHEMA, 'profiles': cache}
    tmp = PROFILE_CACHE_FILE.with_suffix('.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(blob, f, ensure_ascii=False)
    tmp.replace(PROFILE_CACHE_FILE)


# ---------- Profile fetcher ----------
def _ensure_college_suffix(url, team_canonical):
    """Append /college-{team_id}/ to a profile URL if it's missing.

    The team-info-section header h2 (origin team) and commit-banner are
    most reliably rendered when the URL includes the destination college
    suffix. Roster pages sometimes give us bare /player/{slug}-{id}/ URLs
    which render a sparse layout.
    """
    if not url:
        return url
    if '/college-' in url:
        return url
    try:
        from team_urls import TEAM_URLS
        info = TEAM_URLS.get(team_canonical)
        if not info:
            return url
        team_id = info[2]
        return url.rstrip('/') + f'/college-{team_id}/'
    except Exception:
        return url


async def fetch_one_profile(context, sem, url, player_id):
    """Fetch & parse a single player profile. Returns the full profile dict
    {origin_team, destination_team, transfer_events, section_titles,
    fetch_status} on success; on terminal failure returns a dict with
    fetch_status='failed' (so we cache the failure and don't re-hammer).
    """
    async with sem:
        for attempt in range(MAX_PROFILE_RETRIES):
            page = await context.new_page()
            # Block heavy assets — we only need the HTML.
            await page.route(
                "**/*.{png,jpg,jpeg,gif,svg,webp,mp4,webm,woff,woff2,ttf,otf,css}",
                lambda route: route.abort(),
            )
            # Block known overlay/ad scripts that can blank out the page.
            for pattern in ("**/*bouncex*", "**/*bounceexchange*",
                            "**/*integralas*", "**/*IL_INSEARCH*"):
                await page.route(pattern, lambda route: route.abort())
            try:
                await asyncio.sleep(random.uniform(PROFILE_DELAY_MIN, PROFILE_DELAY_MAX))
                await page.goto(url, timeout=PROFILE_NAV_TIMEOUT_MS,
                                wait_until='domcontentloaded')
                # Tolerate selector timeout — page may have loaded enough
                # to parse without the rankings section ever arriving.
                try:
                    await page.wait_for_selector(
                        'section.rankings-section, .name, h1.name',
                        timeout=PROFILE_SELECTOR_TIMEOUT_MS,
                    )
                except PlaywrightTimeoutError:
                    pass
                html = await page.content()
                await page.close()
                if len(html) < 2000:
                    raise RuntimeError("suspiciously small profile HTML")
                profile = parse_player_profile(html)
                profile['fetch_status'] = 'ok'
                return profile
            except Exception as e:
                try:
                    await page.close()
                except Exception:
                    pass
                if attempt < MAX_PROFILE_RETRIES - 1:
                    await asyncio.sleep(2 + attempt * 2)
                else:
                    print(f"    profile FAIL ({player_id}): {type(e).__name__}: {e}")
                    return {
                        'origin_team': '', 'destination_team': '',
                        'transfer_events': [], 'prospect_event': None,
                        'section_titles': [],
                        'fetch_status': 'failed',
                    }
        return {
            'origin_team': '', 'destination_team': '',
            'transfer_events': [], 'prospect_event': None,
            'section_titles': [],
            'fetch_status': 'failed',
        }


def _apply_prospect_to_row(row, profile):
    """Write HS recruiting / JUCO fields onto the row, if available.

    Runs INDEPENDENTLY of transfer logic. A player can have HS recruiting
    data even if they never transferred, and vice-versa. No-op if the
    profile has no prospect_event.
    """
    pe = profile.get('prospect_event')
    if not pe:
        return
    row['hs_class_year']    = str(pe.get('year', '') or '')
    row['hs_rating']        = pe.get('rating', '')
    row['hs_national_rank'] = pe.get('national_rank', '')
    row['hs_position_rank'] = pe.get('position_rank', '')
    row['hs_position']      = pe.get('position', '')
    row['hs_stars']         = pe.get('stars', '')
    row['hs_section_kind']  = pe.get('kind', '')


def apply_profile_to_row(row, profile, season):
    """Write profile-derived fields onto the row.

    Sets profile_scraped to one of:
        ok               — transfer event found & applied
        ok_no_transfer   — profile fetched OK but no transfer event applies
                            (HS fields may still be populated)
        failed           — profile fetch failed (HS/transfer fields blank)

    HS recruiting fields are written regardless of transfer status — a
    player's HS recruiting data is independent of whether they transferred.
    """
    # Failed fetch — nothing to apply, leave all enrichment fields blank
    if profile.get('fetch_status') == 'failed':
        row['profile_scraped'] = 'failed'
        return

    # Always apply HS recruiting data when present (independent of transfer)
    _apply_prospect_to_row(row, profile)

    event = pick_transfer_event(profile, season)
    if event is None:
        row['profile_scraped'] = 'ok_no_transfer'
        # Still record origin/destination teams (they're profile-level, not
        # event-level) so analysts can see "this player is on Alabama, came
        # from Auburn" even without a transfer event recorded.
        row['transfer_origin_team']      = profile.get('origin_team', '')
        row['transfer_destination_team'] = profile.get('destination_team', '')
        return

    row['profile_scraped']           = 'ok'
    row['transfer_origin_team']      = profile.get('origin_team', '')
    row['transfer_destination_team'] = profile.get('destination_team', '')
    row['transfer_rating']           = event.get('rating', '')
    row['transfer_overall_rank']     = event.get('overall_rank', '')
    row['transfer_position_rank']    = event.get('position_rank', '')
    row['transfer_position']         = event.get('position', '')
    row['transfer_class_year']       = str(event.get('year', '') or '')
    row['transfer_stars']            = event.get('stars', '')


async def enrich_with_profiles(rows, context, profile_cache, concurrency, season):
    """For every row missing a profile in cache, fetch the profile.
    Then apply the season-appropriate transfer event to each row.
    Mutates rows in-place. Returns the count of newly-cached profiles.
    """
    # 1. Determine which (player_id, url) combos need fetching
    needed = {}     # player_id -> url
    for r in rows:
        pid = r.get('247_id') or ''
        url = r.get('profile_url') or ''
        if not pid:
            r['profile_scraped'] = 'no_url'
            continue
        if pid in profile_cache:
            continue   # already cached, will apply below
        if not url:
            r['profile_scraped'] = 'no_url'
            continue
        # Append /college-{team_id}/ if missing — helps team-info-section render
        full_url = _ensure_college_suffix(url, r.get('team', ''))
        needed[pid] = full_url

    # 2. Fetch missing profiles concurrently
    new_count = 0
    if needed:
        sem = asyncio.Semaphore(concurrency)
        tasks = [fetch_one_profile(context, sem, url, pid)
                 for pid, url in needed.items()]
        results = await asyncio.gather(*tasks)
        for (pid, _url), res in zip(needed.items(), results):
            profile_cache[pid] = res
            if res.get('fetch_status') == 'ok':
                new_count += 1

    # 3. Apply cache to rows (season-aware event selection)
    for r in rows:
        pid = r.get('247_id') or ''
        if pid and pid in profile_cache:
            apply_profile_to_row(r, profile_cache[pid], season)
    return new_count


# ---------- Roster page scrape ----------
async def scrape_team_season(page, team, season, verbose=False):
    """Scrape one (team, season) page. Returns list of row dicts on success,
    or None on non-recoverable failure. Caller handles retries."""
    url = team_url(team, season)
    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=NAV_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        if verbose:
            print(f"  TIMEOUT goto {team} {season}")
        return None
    except Exception as e:
        if verbose:
            print(f"  ERROR goto {team} {season}: {type(e).__name__}: {e}")
        return None

    try:
        await page.wait_for_selector('table', timeout=10000)
    except PlaywrightTimeoutError:
        if verbose:
            print(f"  NO TABLE for {team} {season}")
        return []   # valid empty — pre-FBS season

    html = await page.content()
    return parse_roster_html(html, team, season)


# ---------- Orchestrator ----------
async def run(seasons, teams, output, skip_existing=True,
              skip_profiles=False, profile_concurrency=DEFAULT_PROFILE_CONCURRENCY):
    CHECKPOINT_DIR.mkdir(exist_ok=True)
    for s in seasons:
        (CHECKPOINT_DIR / str(s)).mkdir(parents=True, exist_ok=True)

    profile_cache = {} if skip_profiles else load_profile_cache()

    tasks = []
    for season in seasons:
        for team in teams:
            if not is_fbs_in_year(team, season):
                continue
            tasks.append((team, season))

    print(f"Tasks to run: {len(tasks)}  |  profile enrichment: "
          f"{'OFF' if skip_profiles else f'ON (concurrency={profile_concurrency})'}")
    completed = 0
    skipped = 0
    consecutive_failures = 0
    started_at = time.time()
    new_profiles_this_run = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx_kwargs = dict(user_agent=random.choice(USER_AGENTS),
                          viewport={'width': 1280, 'height': 900})
        context = await browser.new_context(**ctx_kwargs)
        page = await context.new_page()

        for i, (team, season) in enumerate(tasks):
            team_safe = team.replace(' ', '_').replace('/', '_').replace('(', '').replace(')', '')
            ckpt = CHECKPOINT_DIR / str(season) / f"{team_safe}.csv"

            if skip_existing and ckpt.exists():
                # Validate the existing checkpoint. Forced re-scrape if any of:
                #   - missing the new transfer columns
                #   - poisoned by the legacy doubled-URL bug ('comhttps' in URLs)
                #   - poisoned by the legacy 'profile_scraped=failed' bug
                #     (>50% of profiled rows failed because of doubled URLs)
                #   - height column is date-corrupted (e.g. '2-Jun', '10-May')
                try:
                    head = pd.read_csv(ckpt, nrows=1)
                    has_transfer_cols = 'transfer_origin_team' in head.columns
                    sample = pd.read_csv(
                        ckpt,
                        usecols=lambda c: c in ('profile_url', 'profile_scraped', 'height'),
                        dtype=str,
                    ).fillna('')
                    bad_url_share = (
                        sample['profile_url']
                        .str.contains('comhttps', na=False).mean()
                        if 'profile_url' in sample.columns else 0
                    )
                    fail_share = (
                        (sample['profile_scraped'] == 'failed').mean()
                        if 'profile_scraped' in sample.columns else 0
                    )
                    height_corrupt = (
                        sample['height']
                        .str.contains(r'May|Jun|Jul|Aug', regex=True, na=False).any()
                        if 'height' in sample.columns else False
                    )
                    poisoned = (bad_url_share > 0.1
                                or fail_share > 0.5
                                or height_corrupt)
                except Exception:
                    has_transfer_cols = False
                    poisoned = False
                if poisoned:
                    print(f"[{i+1}/{len(tasks)}] REDO {team} {season} "
                          f"(checkpoint poisoned by known bug — re-scraping)")
                elif has_transfer_cols or skip_profiles:
                    skipped += 1
                    if i % 20 == 0:
                        print(f"[{i+1}/{len(tasks)}] SKIP {team} {season} (cached)")
                    continue
                else:
                    print(f"[{i+1}/{len(tasks)}] BACKFILL {team} {season} "
                          f"(checkpoint exists but missing transfer cols)")

            # Rotate user-agent every ~30 requests (new context)
            if i > 0 and i % 30 == 0:
                await context.close()
                context = await browser.new_context(
                    user_agent=random.choice(USER_AGENTS),
                    viewport={'width': 1280, 'height': 900},
                )
                page = await context.new_page()

            # ---- Scrape the roster page ----
            rows = None
            for attempt in range(MAX_RETRIES_PER_PAGE):
                try:
                    rows = await scrape_team_season(page, team, season, verbose=(attempt > 0))
                    if rows is not None:
                        break
                except Exception as e:
                    print(f"  retry {attempt+1}: {type(e).__name__}: {e}")
                await asyncio.sleep(5 + attempt * 3)

            status = ''
            if rows is None:
                consecutive_failures += 1
                status = 'FAIL'
                with open(ckpt, 'w', newline='') as f:
                    f.write('# SCRAPE FAILED — delete this file to retry\n')
            else:
                consecutive_failures = 0
                if rows:
                    # ---- Enrich with profile data ----
                    if not skip_profiles:
                        try:
                            new_n = await enrich_with_profiles(
                                rows, context, profile_cache,
                                profile_concurrency, season,
                            )
                            new_profiles_this_run += new_n
                            if new_n:
                                flush_profile_cache(profile_cache)
                        except Exception as e:
                            print(f"  WARN: profile enrichment failed for "
                                  f"{team} {season}: {type(e).__name__}: {e}")
                    pd.DataFrame(rows).reindex(columns=ALL_OUTPUT_COLS).to_csv(
                        ckpt, index=False)
                    status = f'OK ({len(rows)} rows)'
                else:
                    pd.DataFrame(columns=ALL_OUTPUT_COLS).to_csv(ckpt, index=False)
                    status = 'OK (0 rows)'

            completed += 1
            elapsed = time.time() - started_at
            rate = completed / elapsed if elapsed > 0 else 0
            eta_s = (len(tasks) - skipped - completed) / rate if rate > 0 else 0
            cache_note = (f"  cache={len(profile_cache):,}"
                          f" (+{new_profiles_this_run} new)") if not skip_profiles else ""
            print(f"[{i+1}/{len(tasks)}] {team:24s} {season}  {status}   "
                  f"elapsed={elapsed/60:.1f}m  eta={eta_s/60:.0f}m{cache_note}")

            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                print(f"\n⚠  ABORTING: {MAX_CONSECUTIVE_FAILURES} consecutive failures. "
                      f"Likely rate-limited or structural change. Inspect & rerun.")
                break

            await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        await browser.close()

    # Final cache flush
    if not skip_profiles:
        flush_profile_cache(profile_cache)

    # Consolidate all checkpoints into the final CSV
    print(f"\nConsolidating checkpoints → {output}")
    dfs = []
    for season in seasons:
        season_dir = CHECKPOINT_DIR / str(season)
        for ckpt in sorted(season_dir.glob('*.csv')):
            try:
                df = pd.read_csv(ckpt)
                if len(df):
                    dfs.append(df)
            except Exception as e:
                print(f"  WARN: could not read {ckpt}: {e}")
    if dfs:
        full = pd.concat(dfs, ignore_index=True)
        full = full.reindex(columns=ALL_OUTPUT_COLS)
        full.to_csv(output, index=False)
        print(f"Wrote {len(full):,} total rows to {output}")

        # Post-run validation
        print(f"\n=== Post-run validation ===")
        print(f"Distinct 247 IDs: {full['247_id'].nunique():,}")
        print(f"Rows with 247 ID:    {full['247_id'].notna().sum():,}  "
              f"({full['247_id'].notna().mean():.1%})")
        print(f"Rows without (unrated): {full['247_id'].isna().sum():,}")
        if not skip_profiles:
            for f in ['transfer_origin_team', 'transfer_rating',
                      'transfer_overall_rank', 'transfer_class_year']:
                filled = full[f].astype(str).str.strip().replace('nan', '').ne('').sum()
                print(f"  {f:30s} filled: {filled:6,}  ({filled/len(full):.1%})")
        print(f"\nRows per season:")
        print(full.groupby('season').size().to_string())
        print(f"\nTeams with <40 or >200 roster rows (suspicious):")
        team_season_counts = full.groupby(['team', 'season']).size()
        odd = team_season_counts[(team_season_counts < 40) | (team_season_counts > 200)]
        if len(odd):
            print(odd.to_string())
        else:
            print("  (none — all team-seasons have 40-200 rows, nominal)")
    else:
        print("No data collected.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--seasons', nargs='+', type=int, required=True,
                    help='Seasons to scrape, e.g. --seasons 2018 2019 2020')
    ap.add_argument('--teams', nargs='+', default=None,
                    help='Teams to scrape (canonical name). Default = all FBS.')
    ap.add_argument('--output', default='roster_full.csv')
    ap.add_argument('--force', action='store_true',
                    help='Ignore checkpoints, re-scrape all')
    ap.add_argument('--skip-profiles', action='store_true',
                    help='Skip per-player profile enrichment (much faster, '
                         'omits transfer fields).')
    ap.add_argument('--profile-concurrency', type=int,
                    default=DEFAULT_PROFILE_CONCURRENCY,
                    help='Concurrent profile fetches per team-season '
                         f'(default {DEFAULT_PROFILE_CONCURRENCY}).')
    args = ap.parse_args()

    teams = args.teams if args.teams else all_teams()
    bad = [t for t in teams if t not in TEAM_URLS]
    if bad:
        print(f"Unknown teams: {bad}")
        sys.exit(1)

    asyncio.run(run(
        args.seasons, teams, args.output,
        skip_existing=not args.force,
        skip_profiles=args.skip_profiles,
        profile_concurrency=args.profile_concurrency,
    ))


if __name__ == '__main__':
    main()
