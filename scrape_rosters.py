"""
Scrape active rosters for all FBS teams across seasons from 247sports,
then enrich each roster row by visiting the player profile to capture
HS recruiting metadata and transfer-portal metadata.

NFL draft info is NOT scraped — 247sports player profiles do not include
draft data. (If a "Drafted by X, Round Y, Pick Z" mention appears anywhere
on the page, it's from the global site navigation chrome, not the player.)

Rating columns:
  - roster_rating:       the Rating cell from the team roster table, exactly
                         as 247 displays it. Historically a decimal composite
                         (0.8622); since mid-2026 247 renders it as a whole
                         number on the 0-100 scale (86). Captured verbatim.
  - hs_composite_rating: populated ONLY when the roster Rating cell is a
                         decimal (0.8622). Blank when 247 shows whole numbers.
  - hs_scout_rating:     the 247Sports scout rating (whole number, e.g. 95),
                         captured from the profile's "247Sports" section.
  - roster_stars:        count of gold star icons in the roster Rating cell
                         (blank if 247 renders no icons there).

Roster page structure (verified Sep 2026):
  The roster is TWO side-by-side <table>s — a one-column "Name" table
  (frozen column) and the Jersey|POS|Height|Weight|Yr|Age|High School|Rating
  data table. Rows pair by index. Players with no 247 profile are plain text
  in the Name table (2026) or link to a placeholder /player/--<id>/ (2025 and
  earlier). The old parser required one <a> per data row and returned 0 rows
  for every 2026 page.

Design principles (from scraping playbook):
  - Playwright + domcontentloaded (NOT networkidle)
  - 247 client-side hydrates the rankings-section ~500ms after DOM ready;
    we wait for .rank-block / .commit-banner content (not the section
    skeleton) and add a post-load sleep.
  - Verify the page's season (from <h1>/<title>) matches the requested one;
    fall back to alternate roster URL forms if 247 redirects.
  - Randomized delays + user-agent rotation
  - NEVER break-on-exception in loops — use continue + failure counter
  - Per-team-season checkpointing for crash recovery; 0-row checkpoints
    are re-scraped on the next run (they are never treated as "done").
  - GLOBAL profile cache keyed by 247_id only — content for a given player
    is consistent enough across team-views that one fetch suffices.
  - Cache flushed to disk after every team-season so a mid-run crash can
    resume without re-scraping any profiles
  - Profile fetches run concurrently (asyncio.Semaphore)
  - Post-run verification (row counts, ID format, null-ratio sanity)
  - On an empty result the raw page HTML is saved to
    checkpoints/<season>/<team>_debug.html so the workflow artifact shows
    exactly what 247 served.

Usage:
  python scrape_rosters.py --seasons 2026 --output roster_2026.csv
  python scrape_rosters.py --seasons 2018 2019 2020 --output roster.csv
  python scrape_rosters.py --teams Troy Alabama --seasons 2022
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
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from team_urls import (TEAM_URLS, team_url, roster_url_candidates,
                       is_fbs_in_year, all_teams)

# ---------- Config ----------
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

DELAY_MIN = 1.0
DELAY_MAX = 3.0
NAV_TIMEOUT_MS = 30000
TABLE_SELECTOR_TIMEOUT_MS = 10000
PROFILE_NAV_TIMEOUT_MS = 30000
PROFILE_SELECTOR_TIMEOUT_MS = 5000
PROFILE_POST_LOAD_SLEEP = 1.5
MAX_CONSECUTIVE_FAILURES = 8
MAX_RETRIES_PER_PAGE = 3     # navigation failures
EMPTY_RETRIES = 1            # extra attempts when a page loads but yields 0 rows
MAX_PROFILE_RETRIES = 2
DEFAULT_PROFILE_CONCURRENCY = 6
PROFILE_DELAY_MIN = 0.3
PROFILE_DELAY_MAX = 1.0

# Bump this whenever the cache schema changes so old caches get thrown out.
# Schema 8 changes:
#   - profile dict now includes both scout rating (whole number from
#     "247Sports" section) and composite handling (decimal from
#     "247Sports Composite®" section as scout fallback)
#   - position label captured even when rank value is N/A (matches
#     transfer scraper logic)
PROFILE_CACHE_SCHEMA = 8

CHECKPOINT_DIR = Path("checkpoints")
PROFILE_CACHE_FILE = CHECKPOINT_DIR / "profiles_cache.json"

# Regex to extract 247 ID from player URL, e.g. /player/carlton-martial-91227/
PLAYER_URL_RE = re.compile(r'/player/([^/]*?)-(\d+)/?$')

# Roster-page <h1> looks like "2026 Arkansas Razorbacks Football Roster";
# <title> looks like "Arkansas Razorbacks 2026 Rosters".
PAGE_YEAR_RE = re.compile(r'\b(20\d{2})\b')

TRANSFER_FIELDS = [
    'transfer_origin_team',
    'transfer_destination_team',
    'transfer_rating',
    'transfer_overall_rank',
    'transfer_position_rank',
    'transfer_position',
    'transfer_class_year',
    'transfer_stars',
]

HS_FIELDS = [
    'hs_class_year',           # (YYYY) from the prospect rank-block
    'hs_composite_rating',     # DECIMAL composite rating from roster table (~0.8622) — blank when 247 shows whole numbers
    'hs_scout_rating',         # WHOLE NUMBER scout rating from profile (~95)
    'hs_national_rank',        # national rank — JUCO ranks suffixed " (JUCO)"
    'hs_position_rank',        # position rank — JUCO ranks suffixed " (JUCO)"
    'hs_position',             # position label from prospect section
    'hs_stars',                # 1-5 or 'JUCO'
    'hs_section_kind',         # '247Sports' or '247Sports Composite' or 'JUCO'
]

# profile_scraped is one of:
#   'ok'              — fetched + parsed; matching transfer event applied
#   'ok_no_transfer'  — fetched + parsed but no transfer event applies
#   'failed'          — fetch error after all retries
#   'no_url'          — row had no profile URL (placeholder/walk-on)
#   'skipped'         — enrichment was disabled
STATUS_FIELDS = ['profile_scraped']

# Captured straight from the roster table (no profile visit needed).
ROSTER_EXTRA_FIELDS = [
    'roster_rating',           # Rating cell verbatim: "86" (2026+) or "0.8622" (older)
    'roster_stars',            # gold star icons in the Rating cell, if any
    'practice_squad',          # 'Y' when the name carries the (PS) marker
]

ALL_OUTPUT_COLS = [
    '247_id', 'player_name', 'team', 'season', 'jersey', 'position',
    'height', 'weight', 'class_yr', 'age', 'high_school',
    'profile_url', 'scrape_ts',
] + HS_FIELDS + TRANSFER_FIELDS + STATUS_FIELDS + ROSTER_EXTRA_FIELDS


def _now_iso():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _is_na(s):
    """True if a parsed value should be treated as missing for numeric fields.
    247 renders 'N/A' (sometimes whitespace-padded) for sections that have
    no data — we don't want to store those as if they were real values for
    ranks, ratings, stars. Position LABELS are still valid even when their
    accompanying numeric is N/A — those are handled separately.
    """
    if s is None:
        return True
    s = str(s).strip().upper()
    return s in ('', 'N/A', 'NA', '—', '–', '-')


# ---------- Roster page extraction ----------
def page_year_from_html(html):
    """Return the season the roster page claims to be for, or None.

    Checks every <h1> containing 'roster', then <title>, then og:title.
    """
    soup = BeautifulSoup(html, 'lxml')
    candidates = []
    for h1 in soup.find_all('h1'):
        txt = h1.get_text(' ', strip=True)
        if 'roster' in txt.lower():
            candidates.append(txt)
    if soup.title and soup.title.string:
        candidates.append(soup.title.string)
    og = soup.find('meta', attrs={'property': 'og:title'})
    if og and og.get('content'):
        candidates.append(og['content'])
    for txt in candidates:
        m = PAGE_YEAR_RE.search(txt or '')
        if m:
            return int(m.group(1))
    return None


def _anchor_info(a):
    """Return (247_id, profile_url) for a player anchor, or (None, None) for
    placeholder / non-player links."""
    if not a or not a.get('href'):
        return None, None
    href = a['href']
    m = PLAYER_URL_RE.search(href)
    if not m:
        return None, None
    slug, pid = m.group(1), m.group(2)
    is_placeholder = (
        pid == '46120272'
        or slug == ''
        or set(slug) <= {'-'}
    )
    if is_placeholder:
        return None, None
    if href.startswith('http://') or href.startswith('https://'):
        full_url = href.rstrip('/') + '/'
    else:
        full_url = f"https://247sports.com{href.rstrip('/')}/"
    return pid, full_url


def _name_cell(cell):
    """Parse a Name cell → dict(name, 247_id, profile_url, practice_squad)."""
    a = None
    for cand in cell.find_all('a', href=True):
        if PLAYER_URL_RE.search(cand['href']):
            a = cand
            break
    name = cell.get_text(' ', strip=True)
    if not name and a:
        name = a.get_text(strip=True)
    ps = ''
    m_ps = re.search(r'\(\s*PS\s*\)', name, flags=re.I)
    if m_ps:
        ps = 'Y'
        name = (name[:m_ps.start()] + name[m_ps.end():]).strip()
    name = re.sub(r'\s+', ' ', name).strip()
    pid, url = _anchor_info(a)
    return {'name': name, '247_id': pid, 'profile_url': url, 'practice_squad': ps}


def _rating_cell(cell):
    """Parse the Rating cell → (composite_decimal, raw_rating, stars)."""
    text = cell.get_text(' ', strip=True) if cell is not None else ''
    composite = ''
    raw = ''
    if text and not _is_na(text):
        m_dec = re.match(r'^\s*(\d+\.\d+)', text)
        if m_dec:
            composite = m_dec.group(1)
            raw = composite
        else:
            m_int = re.match(r'^\s*(\d+)', text)
            if m_int:
                raw = m_int.group(1)
    stars = ''
    if cell is not None:
        icons = cell.select('span.icon-starsolid.yellow')
        if not icons:
            icons = [s for s in cell.select('[class*="star"]')
                     if 'yellow' in ' '.join(s.get('class', []))]
        if icons:
            stars = str(min(len(icons), 5))
    return composite, raw, stars


def _header_cells(table):
    trs = table.find_all('tr')
    if not trs:
        return [], trs
    return [c.get_text(' ', strip=True).lower() for c in trs[0].find_all(['th', 'td'])], trs


def parse_roster_html(html, team, season, diag=None):
    """Extract roster rows from a team roster page.

    The page renders two tables: a one-column "Name" table and the data
    table (Jersey | POS | Height | Weight | Yr | Age | High School | Rating).
    They pair by row index. Names without a 247 profile may be plain text
    (no <a>) — those rows are kept with 247_id/profile_url blank.

    `diag`, if given, receives counts and a `reason` string when 0 rows.
    """
    diag = diag if diag is not None else {}
    soup = BeautifulSoup(html, 'lxml')

    name_table = None
    data_table = None
    tables = soup.find_all('table')
    diag['n_tables'] = len(tables)
    for table in tables:
        header, trs = _header_cells(table)
        if len(trs) < 2:
            continue
        header_text = ' '.join(header)
        if data_table is None and ('jersey' in header_text or 'height' in header_text
                                   or re.search(r'\bpos\b', header_text)):
            data_table = table
            continue
        if name_table is None and header and header[0] == 'name' and len(header) <= 2:
            name_table = table
            continue

    if data_table is None:
        diag['n_name_rows'] = 0
        diag['n_data_rows'] = 0
        diag['n_anchors'] = sum(1 for a in soup.find_all('a', href=True)
                                if PLAYER_URL_RE.search(a['href']))
        diag['reason'] = ('no roster data table found (no <table> with a '
                          'Jersey/POS/Height header)')
        return []

    # ---- data rows (keep the cell elements, we need the Rating cell) ----
    data_header, data_trs = _header_cells(data_table)
    name_in_data = bool(data_header) and data_header[0] == 'name'
    data_rows = []
    for tr in data_trs[1:]:
        cells = tr.find_all(['td', 'th'])
        if len(cells) < (7 if name_in_data else 6):
            continue
        data_rows.append(cells)
    diag['n_data_rows'] = len(data_rows)

    # ---- name rows ----
    name_rows = []
    if name_in_data:
        name_rows = [_name_cell(cells[0]) for cells in data_rows]
        data_rows = [cells[1:] for cells in data_rows]
    elif name_table is not None:
        _, name_trs = _header_cells(name_table)
        for tr in name_trs[1:]:
            cells = tr.find_all(['td', 'th'])
            if not cells:
                continue
            name_rows.append(_name_cell(cells[0]))
    diag['n_name_rows'] = len(name_rows)
    diag['n_anchors'] = sum(1 for r in name_rows if r['247_id'])

    if not data_rows:
        diag['reason'] = 'data table found but it has no player rows'
        return []

    if not name_rows:
        # Fallback: legacy layout — pair every player <a> on the page with
        # the data rows using the best contiguous window (old behaviour).
        anchors = []
        for a in soup.find_all('a', href=True):
            if not PLAYER_URL_RE.search(a['href']):
                continue
            nm = a.get_text(strip=True)
            if not nm:
                continue
            pid, url = _anchor_info(a)
            anchors.append({'name': nm, '247_id': pid, 'profile_url': url,
                            'practice_squad': ''})
        n = len(data_rows)
        if len(anchors) < n:
            diag['n_anchors'] = len(anchors)
            diag['reason'] = (f'no Name table and only {len(anchors)} player '
                              f'links for {n} data rows')
            return []
        best = None
        for i in range(len(anchors) - n + 1):
            window = anchors[i:i + n]
            score = sum(1 for a in window if a['247_id'])
            if best is None or score > best[0]:
                best = (score, window)
        name_rows = best[1]
        diag['n_name_rows'] = len(name_rows)
        diag['n_anchors'] = best[0]
        diag['pairing'] = 'anchor-window fallback'
    elif len(name_rows) != len(data_rows):
        diag['pairing'] = (f'WARNING row-count mismatch: {len(name_rows)} names vs '
                           f'{len(data_rows)} data rows — paired by index up to the shorter list')
        print(f"  WARN {team} {season}: {diag['pairing']}")
    else:
        diag['pairing'] = 'name-table + data-table by index'

    rows = []
    ts = _now_iso()
    for nm, cells in zip(name_rows, data_rows):
        # Roster table columns: Jersey | POS | Height | Weight | Yr | Age | HS | Rating
        texts = [c.get_text(' ', strip=True) for c in cells]
        texts = (texts + [''] * 8)[:8]
        jersey, pos, height, weight, yr, age, hs, _ = texts
        rating_cell = cells[7] if len(cells) > 7 else None
        composite, raw_rating, stars = _rating_cell(rating_cell)
        # Convert "6-2" → 6'2" so Excel doesn't auto-date as 2-Jun
        m_h = re.match(r'^\s*(\d{1,2})-(\d{1,2})\s*$', height or '')
        if m_h:
            height = f"{m_h.group(1)}'{m_h.group(2)}\""
        if _is_na(hs):
            hs = ''
        row = {
            '247_id': nm['247_id'],
            'player_name': nm['name'],
            'team': team,
            'season': season,
            'jersey': jersey,
            'position': pos,
            'height': height,
            'weight': weight,
            'class_yr': yr,
            'age': age,
            'high_school': hs,
            'profile_url': nm['profile_url'] or '',
            'scrape_ts': ts,
        }
        for f in HS_FIELDS + TRANSFER_FIELDS + STATUS_FIELDS + ROSTER_EXTRA_FIELDS:
            row[f] = ''
        # Pre-populate ratings from roster table (canonical source)
        row['hs_composite_rating'] = composite
        row['roster_rating'] = raw_rating
        row['roster_stars'] = stars
        row['practice_squad'] = nm['practice_squad']
        rows.append(row)

    if not rows:
        diag['reason'] = 'tables found but no rows could be paired'
    return rows


# ---------- Player profile extraction ----------
def _parse_one_section(section, is_juco_title):
    """Parse a single rankings-section into a normalized event dict.

    Captures rating, year, overall_rank, national_rank, position_rank,
    position, stars. The position LABEL is captured even when its rank
    value is N/A (matches transfer scraper logic); numeric values (ranks,
    rating, stars) are N/A-filtered to empty.
    """
    out = {
        'kind': '', 'rating': '', 'year': None,
        'overall_rank': '', 'national_rank': '',
        'position_rank': '', 'position': '', 'stars': '',
    }

    rating_block = section.select_one('.rank-block')
    if rating_block:
        rating_text = rating_block.get_text(' ', strip=True)
        if not _is_na(rating_text):
            m_rating = re.match(r'^\s*(\d+(?:\.\d+)?)', rating_text)
            if m_rating:
                out['rating'] = m_rating.group(1)
        m_year = re.search(r'\((\d{4})\)', rating_text or '')
        if m_year:
            out['year'] = int(m_year.group(1))

    # Stars — JUCO sections don't render gold stars
    if is_juco_title:
        out['stars'] = 'JUCO'
    else:
        stars = section.select('span.icon-starsolid.yellow')
        if stars:
            out['stars'] = str(min(len(stars), 5))

    # Ranks: <li><b>LABEL</b><strong>VALUE</strong></li>
    # For position rows: capture the LABEL even if the rank VALUE is N/A.
    # For all other rank types (OVR, NATL): only capture when value is real.
    for li in section.select('li'):
        bold = li.find('b')
        strong = li.find('strong')
        if not bold or not strong:
            continue
        label = bold.get_text(strip=True).upper()
        value = strong.get_text(strip=True)
        value_is_na = _is_na(value)
        link = li.find('a')
        href = (link.get('href', '') if link else '')

        if 'OVR' in label or 'OVERALL' in label:
            if not value_is_na:
                out['overall_rank'] = value
        elif 'NATL' in label or 'NATIONAL' in label:
            if not value_is_na:
                out['national_rank'] = value
        elif 'State=' in href:
            continue
        elif not out['position']:
            # First non-OVR, non-NATL, non-state row → position label.
            # Capture label even when value is N/A. Only store the rank
            # number when it's a real value.
            out['position'] = label
            if not value_is_na:
                out['position_rank'] = value
    return out


def _prospect_event_is_real(ev):
    """Determine whether a parsed prospect event has any actual data."""
    if not ev:
        return False
    return any([
        ev.get('rating'),
        ev.get('overall_rank'),
        ev.get('national_rank'),
        ev.get('position_rank'),
        ev.get('year'),
        ev.get('stars') and ev.get('stars') not in ('0',),
    ])


def parse_player_profile(html):
    """
    Pull origin/destination teams, ALL transfer events, and a prospect event
    from a profile.

    Section title variants observed on live profiles (verified via diagnostic):
      - "247Sports Transfer Rankings"  → transfer (year-specific event)
      - "247Sports"                    → HS recruiting (whole-number scout rating)
      - "247Sports Composite®"         → HS composite (decimal rating, often for
                                          older players where "247Sports" is N/A)
      - "JUCO"                         → JUCO recruiting

    Prospect preference: JUCO > 247Sports (when real) > Composite > whichever
    exists, even if empty. Position label retained even from empty sections.
    """
    soup = BeautifulSoup(html, 'lxml')
    result = {
        'origin_team': '',
        'destination_team': '',
        'transfer_events': [],
        'prospect_event': None,
        'section_titles': [],
    }

    # Origin team — .team-info-section header h2 (NOT .team-block)
    team_header = soup.select_one('.team-info-section header h2')
    if team_header:
        result['origin_team'] = team_header.get_text(strip=True)

    # Destination team — commit banner
    commit_banner = soup.select_one('.commit-banner span')
    if commit_banner:
        txt = commit_banner.get_text(strip=True)
        if txt and txt.lower() != 'commit':
            result['destination_team'] = txt

    juco_event = None
    primary_247_event = None
    composite_event = None

    for section in soup.select('section.rankings-section'):
        title_tag = section.select_one('h3.title') or section.select_one('h3')
        if not title_tag:
            continue
        title = title_tag.get_text(strip=True)
        result['section_titles'].append(title)

        is_juco = 'JUCO' in title

        if 'Transfer' in title:
            ev = _parse_one_section(section, is_juco_title=False)
            ev['kind'] = 'Transfer'
            result['transfer_events'].append(ev)
        elif is_juco:
            ev = _parse_one_section(section, is_juco_title=True)
            ev['kind'] = 'JUCO'
            if juco_event is None:
                juco_event = ev
        elif title.startswith('247Sports'):
            if 'Composite' in title:
                ev = _parse_one_section(section, is_juco_title=False)
                ev['kind'] = '247Sports Composite'
                if composite_event is None:
                    composite_event = ev
            else:
                ev = _parse_one_section(section, is_juco_title=False)
                ev['kind'] = '247Sports'
                if primary_247_event is None:
                    primary_247_event = ev

    # Prospect event preference order
    if juco_event:
        result['prospect_event'] = juco_event
    elif _prospect_event_is_real(primary_247_event):
        result['prospect_event'] = primary_247_event
    elif _prospect_event_is_real(composite_event):
        result['prospect_event'] = composite_event
    elif primary_247_event:
        # Keep even an empty section so position labels still get through
        result['prospect_event'] = primary_247_event
    elif composite_event:
        result['prospect_event'] = composite_event

    return result


def pick_transfer_event(profile, season):
    """Choose the transfer event for this roster season.

    Prefers events at-or-before the season (most recent winning). Falls back
    to events with year=None if no dated events match.
    """
    events = profile.get('transfer_events') or []
    if not events:
        return None
    dated = [e for e in events if e.get('year') and e['year'] <= season]
    if dated:
        dated.sort(key=lambda e: e.get('year') or 0)
        return dated[-1]
    undated = [e for e in events if not e.get('year')]
    if undated:
        return undated[-1]
    return None


# ---------- Profile cache ----------
def load_profile_cache():
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
                print(f"  Profile cache has {failed/len(cache):.0%} failures — discarding.")
                return {}
        print(f"  Loaded {len(cache):,} cached profiles from {PROFILE_CACHE_FILE}")
        return cache
    except Exception as e:
        print(f"  WARN: could not read profile cache ({e}); starting empty")
        return {}


def flush_profile_cache(cache):
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
    Required for .team-info-section and .commit-banner to render."""
    if not url:
        return url
    if '/college-' in url:
        return url
    try:
        info = TEAM_URLS.get(team_canonical)
        if not info:
            return url
        team_id = info[2]
        return url.rstrip('/') + f'/college-{team_id}/'
    except Exception:
        return url


async def fetch_one_profile(context, sem, url, player_id):
    """Fetch & parse a single player profile."""
    async with sem:
        for attempt in range(MAX_PROFILE_RETRIES):
            page = await context.new_page()
            await page.route(
                "**/*.{png,jpg,jpeg,gif,svg,webp,mp4,webm,woff,woff2,ttf,otf,css}",
                lambda route: route.abort(),
            )
            for pattern in ("**/*bouncex*", "**/*bounceexchange*",
                            "**/*integralas*", "**/*IL_INSEARCH*"):
                await page.route(pattern, lambda route: route.abort())
            try:
                await asyncio.sleep(random.uniform(PROFILE_DELAY_MIN, PROFILE_DELAY_MAX))
                await page.goto(url, timeout=PROFILE_NAV_TIMEOUT_MS,
                                wait_until='domcontentloaded')
                # Wait for the rankings section's CONTENT (rank-block / commit-banner)
                # to hydrate, not just the skeleton section title.
                try:
                    await page.wait_for_selector(
                        '.rank-block, .commit-banner',
                        timeout=PROFILE_SELECTOR_TIMEOUT_MS,
                    )
                except PlaywrightTimeoutError:
                    pass
                await asyncio.sleep(PROFILE_POST_LOAD_SLEEP)
                html = await page.content()
                await page.close()
                if len(html) < 2000:
                    raise RuntimeError("suspiciously small profile HTML")
                profile = parse_player_profile(html)
                profile['fetch_status'] = 'ok'
                profile['fetched_url'] = url
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
                        'fetch_status': 'failed', 'fetched_url': url,
                    }
        return {
            'origin_team': '', 'destination_team': '',
            'transfer_events': [], 'prospect_event': None,
            'section_titles': [],
            'fetch_status': 'failed', 'fetched_url': url,
        }


def _apply_juco_suffix(value, kind):
    """Return value with ' (JUCO)' suffix when section kind is JUCO.
    Skips empty values and N/A so we don't get 'N/A (JUCO)'.
    """
    if not value or _is_na(value):
        return value
    if kind == 'JUCO' and '(JUCO)' not in str(value):
        return f"{value} (JUCO)"
    return value


def apply_profile_to_row(row, profile, season):
    """Write HS and transfer fields from profile into the row.

    IMPORTANT: hs_composite_rating / roster_rating are pre-populated from the
    roster page during parse_roster_html. We do NOT overwrite them here — the
    roster table value is canonical. hs_scout_rating is the profile-parsed value.
    """

    if profile.get('fetch_status') == 'failed':
        row['profile_scraped'] = 'failed'
        return

    # ----- HS / Recruiting (written if prospect_event exists) -----
    pe = profile.get('prospect_event')
    if pe:
        kind = pe.get('kind') or '247Sports'
        row['hs_section_kind'] = kind
        # hs_scout_rating: whole-number rating from profile.
        # Do NOT overwrite hs_composite_rating — that comes from roster table.
        row['hs_scout_rating'] = pe.get('rating', '') or ''
        row['hs_position'] = pe.get('position', '') or ''
        row['hs_stars'] = pe.get('stars', '') or ''
        year = pe.get('year')
        row['hs_class_year'] = str(year) if year else ''
        # JUCO suffix only on real (non-N/A, non-empty) rank values
        row['hs_national_rank'] = _apply_juco_suffix(pe.get('national_rank', '') or '', kind)
        row['hs_position_rank'] = _apply_juco_suffix(pe.get('position_rank', '') or '', kind)

    # ----- Origin / Destination teams (profile-level, season-independent) -----
    row['transfer_origin_team'] = profile.get('origin_team', '') or ''
    row['transfer_destination_team'] = profile.get('destination_team', '') or ''

    # ----- Transfer event (season-specific) -----
    event = pick_transfer_event(profile, season)
    if event is None:
        row['profile_scraped'] = 'ok_no_transfer'
        return

    row['profile_scraped'] = 'ok'
    row['transfer_rating']         = event.get('rating', '') or ''
    row['transfer_overall_rank']   = event.get('overall_rank', '') or ''
    row['transfer_position_rank']  = event.get('position_rank', '') or ''
    row['transfer_position']       = event.get('position', '') or ''
    row['transfer_class_year']     = str(event.get('year', '') or '')
    row['transfer_stars']          = event.get('stars', '') or ''


async def enrich_with_profiles(rows, context, profile_cache, concurrency, season):
    """Fetch any missing profiles and apply them to rows."""
    needed = {}
    for r in rows:
        pid = r.get('247_id') or ''
        url = r.get('profile_url') or ''
        team = r.get('team', '')
        if not pid:
            r['profile_scraped'] = 'no_url'
            continue
        if pid in profile_cache:
            continue
        if not url:
            r['profile_scraped'] = 'no_url'
            continue
        full_url = _ensure_college_suffix(url, team)
        needed[pid] = full_url

    new_count = 0
    if needed:
        sem = asyncio.Semaphore(concurrency)
        tasks = [fetch_one_profile(context, sem, url, pid)
                 for pid, url in needed.items()]
        results = await asyncio.gather(*tasks)
        for pid, res in zip(needed.keys(), results):
            profile_cache[pid] = res
            if res.get('fetch_status') == 'ok':
                new_count += 1

    for r in rows:
        pid = r.get('247_id') or ''
        if not pid:
            continue
        if pid in profile_cache:
            apply_profile_to_row(r, profile_cache[pid], season)
    return new_count


# ---------- Roster page scrape ----------
async def scrape_team_season(page, team, season, verbose=False, diag=None):
    """Scrape one (team, season) page.

    Tries each roster URL form in turn (see team_urls.roster_url_candidates)
    and accepts the first page whose <h1>/<title> season matches `season`
    and that yields rows.

    Returns: list of rows (possibly empty), or None when every URL failed
    to navigate. `diag` receives per-URL details for logging.
    """
    diag = diag if diag is not None else {}
    attempts = []
    nav_failures = 0
    for url in roster_url_candidates(team, season):
        info = {'url': url}
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=NAV_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            info['reason'] = 'goto timeout'
            nav_failures += 1
            attempts.append(info)
            if verbose:
                print(f"  TIMEOUT goto {team} {season} {url}")
            continue
        except Exception as e:
            info['reason'] = f'goto {type(e).__name__}: {e}'
            nav_failures += 1
            attempts.append(info)
            if verbose:
                print(f"  ERROR goto {team} {season} {url}: {type(e).__name__}: {e}")
            continue

        try:
            await page.wait_for_selector('table', timeout=TABLE_SELECTOR_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            pass  # grab whatever rendered; the parser/diag will say why it's empty

        html = await page.content()
        info['final_url'] = page.url
        try:
            info['title'] = (await page.title()) or ''
        except Exception:
            info['title'] = ''
        page_year = page_year_from_html(html)
        info['page_year'] = page_year
        if page_year is not None and page_year != season:
            info['reason'] = f'page is season {page_year}, wanted {season} (redirected?)'
            attempts.append(info)
            continue

        pdiag = {}
        rows = parse_roster_html(html, team, season, diag=pdiag)
        info.update(pdiag)
        if rows:
            diag.update(info)
            diag['attempts'] = attempts
            return rows
        info['reason'] = pdiag.get('reason', 'no rows parsed')
        info['html'] = html
        attempts.append(info)

    diag['attempts'] = attempts
    if attempts and nav_failures == len(attempts):
        return None
    return []


def _describe_attempts(diag):
    lines = []
    for k, a in enumerate(diag.get('attempts', []), 1):
        bits = [f"{k}) {a.get('url')}"]
        if a.get('final_url') and a.get('final_url') != a.get('url'):
            bits.append(f"→ {a['final_url']}")
        if 'title' in a:
            bits.append(f"title={a.get('title')!r}")
        if 'page_year' in a:
            bits.append(f"page_year={a.get('page_year')}")
        if 'n_tables' in a:
            bits.append(f"tables={a.get('n_tables')} names={a.get('n_name_rows')} "
                        f"data_rows={a.get('n_data_rows')} links={a.get('n_anchors')}")
        if a.get('reason'):
            bits.append(f"reason={a['reason']}")
        lines.append('      ' + '  '.join(bits))
    return lines


def _dump_debug_html(diag, season, team_safe):
    html = ''
    for a in reversed(diag.get('attempts', [])):
        if a.get('html'):
            html = a['html']
            break
    if not html:
        return None
    path = CHECKPOINT_DIR / str(season) / f"{team_safe}_debug.html"
    try:
        path.write_text(html, encoding='utf-8')
        return path
    except Exception as e:
        print(f"  WARN: could not write debug html ({e})")
        return None


def _checkpoint_state(ckpt, skip_profiles):
    """Inspect an existing checkpoint. Returns one of:
       'done'      — complete, skip it
       'empty'     — 0 data rows (or a FAILED marker) → re-scrape
       'poisoned'  — looks corrupt → re-scrape
       'backfill'  — old schema → re-scrape
    """
    try:
        head = pd.read_csv(ckpt, nrows=1)
    except Exception:
        return 'poisoned'
    if len(head) == 0:
        return 'empty'
    has_new_schema = (
        'hs_class_year' in head.columns
        and 'hs_composite_rating' in head.columns
        and 'hs_scout_rating' in head.columns
        and 'roster_rating' in head.columns
        and 'draft_year' not in head.columns
    )
    try:
        sample = pd.read_csv(
            ckpt,
            usecols=lambda c: c in ('profile_url', 'profile_scraped',
                                    'height', 'hs_composite_rating',
                                    'roster_rating', 'hs_section_kind'),
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
        skeleton_poisoned = False
        if 'hs_section_kind' in sample.columns:
            kind_set = sample['hs_section_kind'].str.strip().ne('')
            comp_empty = (sample['hs_composite_rating'].str.strip().eq('')
                          if 'hs_composite_rating' in sample.columns
                          else pd.Series(True, index=sample.index))
            raw_empty = (sample['roster_rating'].str.strip().eq('')
                         if 'roster_rating' in sample.columns
                         else pd.Series(True, index=sample.index))
            rating_empty = comp_empty & raw_empty
            if kind_set.sum() > 5:
                skeleton_share = (kind_set & rating_empty).sum() / max(kind_set.sum(), 1)
                skeleton_poisoned = skeleton_share > 0.5
        poisoned = (bad_url_share > 0.1
                    or fail_share > 0.5
                    or height_corrupt
                    or skeleton_poisoned)
    except Exception:
        poisoned = False
    if poisoned:
        return 'poisoned'
    if has_new_schema or skip_profiles:
        return 'done'
    return 'backfill'


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
                state = _checkpoint_state(ckpt, skip_profiles)
                if state == 'done':
                    skipped += 1
                    if i % 20 == 0:
                        print(f"[{i+1}/{len(tasks)}] SKIP {team} {season} (cached)")
                    continue
                elif state == 'empty':
                    print(f"[{i+1}/{len(tasks)}] REDO {team} {season} "
                          f"(checkpoint has 0 rows)")
                elif state == 'poisoned':
                    print(f"[{i+1}/{len(tasks)}] REDO {team} {season} "
                          f"(checkpoint poisoned)")
                else:
                    print(f"[{i+1}/{len(tasks)}] BACKFILL {team} {season} "
                          f"(checkpoint exists but schema mismatch)")

            if i > 0 and i % 30 == 0:
                await context.close()
                context = await browser.new_context(
                    user_agent=random.choice(USER_AGENTS),
                    viewport={'width': 1280, 'height': 900},
                )
                page = await context.new_page()

            rows = None
            diag = {}
            for attempt in range(MAX_RETRIES_PER_PAGE):
                diag = {}
                try:
                    rows = await scrape_team_season(page, team, season,
                                                    verbose=(attempt > 0), diag=diag)
                    if rows:
                        break
                    if rows is not None and attempt >= EMPTY_RETRIES:
                        break   # loaded fine but genuinely empty — accept
                except Exception as e:
                    rows = None
                    print(f"  retry {attempt+1}: {type(e).__name__}: {e}")
                await asyncio.sleep(5 + attempt * 3)

            status = ''
            if rows is None:
                consecutive_failures += 1
                status = 'FAIL'
                for line in _describe_attempts(diag):
                    print(line)
                with open(ckpt, 'w', newline='') as f:
                    f.write('# SCRAPE FAILED — delete this file to retry\n')
            else:
                if rows:
                    consecutive_failures = 0
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
                    linked = sum(1 for r in rows if r.get('247_id'))
                    status = f'OK ({len(rows)} rows, {linked} w/ 247 id)'
                    if diag.get('attempts'):
                        print(f"  note: {team} {season} served by fallback URL "
                              f"{diag.get('url')} after {len(diag['attempts'])} "
                              f"redirected/empty attempt(s):")
                        for line in _describe_attempts(diag):
                            print(line)
                else:
                    consecutive_failures += 1
                    pd.DataFrame(columns=ALL_OUTPUT_COLS).to_csv(ckpt, index=False)
                    status = 'EMPTY (0 rows)'
                    print(f"  EMPTY {team} {season} — what 247 served:")
                    for line in _describe_attempts(diag):
                        print(line)
                    dbg = _dump_debug_html(diag, season, team_safe)
                    if dbg:
                        print(f"      raw page saved to {dbg}")

            completed += 1
            elapsed = time.time() - started_at
            rate = completed / elapsed if elapsed > 0 else 0
            eta_s = (len(tasks) - skipped - completed) / rate if rate > 0 else 0
            cache_note = (f"  cache={len(profile_cache):,}"
                          f" (+{new_profiles_this_run} new)") if not skip_profiles else ""
            print(f"[{i+1}/{len(tasks)}] {team:24s} {season}  {status}   "
                  f"elapsed={elapsed/60:.1f}m  eta={eta_s/60:.0f}m{cache_note}")

            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                print(f"\n⚠  ABORTING: {MAX_CONSECUTIVE_FAILURES} consecutive failures/empties.")
                break

            await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        await browser.close()

    if not skip_profiles:
        flush_profile_cache(profile_cache)

    # ---------- Consolidate all checkpoints ----------
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
        csv_path = str(output).rsplit('.', 1)[0] + '.csv'
        full.to_csv(csv_path, index=False)
        print(f"Wrote {len(full):,} total rows to {csv_path}")
        if str(output).lower().endswith('.xlsx'):
            try:
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    full.to_excel(writer, sheet_name='Players', index=False)
                print(f"Wrote {len(full):,} total rows to {output} (real xlsx, sheet 'Players')")
            except Exception as e:
                print(f"  WARN: failed to write xlsx ({e}); CSV is canonical.")

        # Post-run validation
        print(f"\n=== Post-run validation ===")
        print(f"Distinct 247 IDs:    {full['247_id'].nunique():,}")
        print(f"Rows with 247 ID:    {full['247_id'].notna().sum():,}  "
              f"({full['247_id'].notna().mean():.1%})")
        for f in ROSTER_EXTRA_FIELDS + (HS_FIELDS + TRANSFER_FIELDS if not skip_profiles else []):
            col = full[f]
            filled = (col.notna() & col.astype(str).str.strip().ne('')
                      & ~col.astype(str).str.strip().str.lower().isin(('nan', '<na>', 'none'))).sum()
            print(f"  {f:32s} filled: {filled:6,}  ({filled/len(full):.1%})")
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
    ap.add_argument('--seasons', nargs='+', type=int, required=True)
    ap.add_argument('--teams', nargs='+', default=None)
    ap.add_argument('--output', default='roster_full.csv')
    ap.add_argument('--force', action='store_true',
                    help='Ignore checkpoints, re-scrape all')
    ap.add_argument('--skip-profiles', action='store_true')
    ap.add_argument('--profile-concurrency', type=int,
                    default=DEFAULT_PROFILE_CONCURRENCY)
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
