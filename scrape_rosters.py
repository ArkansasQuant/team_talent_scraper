"""
Scrape active rosters for all FBS teams across seasons from 247sports,
then enrich each roster row by visiting the player profile to capture
HS recruiting metadata and transfer-portal metadata.

NFL draft info is NOT scraped — 247sports player profiles do not include
draft data.

Two rating columns:
  - hs_composite_rating: 247's composite (decimal, e.g. 0.8622), captured
    DIRECTLY from the team talent roster page table — no profile visit
    required. Canonical "247 rating" for analysis.
  - hs_scout_rating: the 247Sports whole-number rating (e.g. 95), captured
    from the profile's "247Sports" rankings section.

Transfer fields are populated ONLY when a "247Sports Transfer Rankings"
section exists on the profile (i.e. the player actually transferred). There
is no transfer_destination column — the roster `team` column already records
which team the player was on each season. transfer_origin_team is best-effort
from the profile's team-info-section.

CRITICAL: We do NOT navigate away from the /college-{team_id}/ view. That
view contains BOTH the "As a Transfer" and "As a Prospect" sections (verified
via live DOM). Earlier versions clicked "View recruiting profile" / "(HS)"
which moved to the HS-only view, destroying transfer capture AND tripling
per-profile load time (causing 5h50m job timeouts).

Design principles:
  - Profile fetch ported from working transfer-portal scraper:
      wait_until="commit", wait_for_selector(".name, h1.name", 15s)
  - Section parser split by kind based on observed DOM:
      Transfer <li>s use positionKey= in href; prospect <li>s use Position=
      State rows have State= (skip); national rank rows have
      InstitutionGroup=HighSchool with no Position= or State=
  - Concurrency 4; no navigation hops → ~2h/season
  - GLOBAL profile cache keyed by 247_id only
  - Cache flushed after each team-season for mid-run resilience
  - Roster-page failures use backoff + higher abort threshold so transient
    247 blocks don't kill a whole season job
  - Post-run verification

Usage:
  python scrape_rosters.py --seasons 2024 --output roster_2024.csv
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
from datetime import datetime
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from team_urls import (TEAM_URLS, team_url, team_url_candidates,
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
PROFILE_NAV_TIMEOUT_MS = 60000
PROFILE_SELECTOR_TIMEOUT_MS = 15000
PROFILE_POST_NAV_WAIT_MS = 800
MAX_CONSECUTIVE_FAILURES = 15        # was 8 — survive transient 247 blocks
ROSTER_BLOCK_BACKOFF_S = 30          # cool-off after a roster-page failure
MAX_RETRIES_PER_PAGE = 3

# --- Roster page readiness -------------------------------------------------
# The 2026 run failed here: every team hit a 10s wait_for_selector('table')
# timeout, returned [], and was logged as "OK (0 rows)". We now wait on a
# roster table OR any player link, allow lazy render to settle, and treat a
# zero-row parse as a FAILURE rather than an empty roster.
ROSTER_READY_SELECTOR = 'table, a[href*="/player/"]'
ROSTER_SELECTOR_TIMEOUT_MS = 20000
LAZY_SETTLE_MS = 1500
EARLY_ABORT_AFTER = 5                # abort if the first N tasks all fail
DEBUG_DIR = Path("debug")
MAX_DEBUG_DUMPS = 8                  # cap artifact size on a systemic failure
MIN_LIST_ROWS = 10                   # non-table fallback needs a real group
MAX_PROFILE_RETRIES = 2
DEFAULT_PROFILE_CONCURRENCY = 4      # no navigation hops → can run at 4
PROFILE_DELAY_MIN = 0.3
PROFILE_DELAY_MAX = 1.0

# Schema 11 changes:
#   - Profile navigation hops REMOVED. We parse the /college-{id}/ view
#     directly, which contains both transfer and prospect sections.
#   - transfer_destination_team column REMOVED entirely.
#   - transfer_* fields populated ONLY when a transfer section exists.
#   - transfer_origin_team captured best-effort from team-info-section.
PROFILE_CACHE_SCHEMA = 11

CHECKPOINT_DIR = Path("checkpoints")
PROFILE_CACHE_FILE = CHECKPOINT_DIR / "profiles_cache.json"

PLAYER_URL_RE = re.compile(r'/player/([^/]*?)-(\d+)/?$')

TRANSFER_FIELDS = [
    'transfer_origin_team',
    'transfer_rating',
    'transfer_overall_rank',
    'transfer_position_rank',
    'transfer_position',
    'transfer_class_year',
    'transfer_stars',
]

HS_FIELDS = [
    'hs_class_year',
    'hs_composite_rating',     # DECIMAL from roster table (~0.8622)
    'hs_scout_rating',         # WHOLE NUMBER from profile (~95 or ~80)
    'hs_national_rank',        # JUCO suffixed " (JUCO)" when applicable
    'hs_position_rank',        # JUCO suffixed " (JUCO)" when applicable
    'hs_position',
    'hs_stars',                # 1-5 or 'JUCO'
    'hs_section_kind',         # '247Sports' or '247Sports Composite' or 'JUCO'
]

STATUS_FIELDS = ['profile_scraped']

ALL_OUTPUT_COLS = [
    '247_id', 'player_name', 'team', 'season', 'jersey', 'position',
    'height', 'weight', 'class_yr', 'age', 'high_school',
    'profile_url', 'scrape_ts',
] + HS_FIELDS + TRANSFER_FIELDS + STATUS_FIELDS


def _is_na(s):
    if s is None:
        return True
    s = str(s).strip().upper()
    return s in ('', 'N/A', 'NA', '—', '–', '-')


def _parse_rank(text):
    if not text:
        return ''
    m = re.search(r'#?\s*([\d,]+)', text)
    if m:
        return m.group(1).replace(',', '')
    return ''


# ---------- Roster page extraction ----------
PLACEHOLDER_IDS = {'46120272'}

ROW_CONTAINER_TAGS = ('tr', 'li', 'article')

CLASS_YR_TOKENS = {
    'FR', 'SO', 'JR', 'SR', 'GR', 'RS', 'RFR', 'RSO', 'RJR', 'RSR',
    'R-FR', 'R-SO', 'R-JR', 'R-SR', 'RS-FR', 'RS-SO', 'RS-JR', 'RS-SR',
    'HS', 'FY',
}

HEIGHT_RE = re.compile(r"^\s*(\d)[-'\u2019](\d{1,2})\"?\s*$")
RATING_RE = re.compile(r'^\s*(0?\.\d{3,4})\s*$')
WEIGHT_RE = re.compile(r'^\s*(\d{2,3})\s*$')
JERSEY_RE = re.compile(r'^\s*#?(\d{1,2})\s*$')


def _anchor_fields(a, m):
    """Identity fields for one player anchor. Absolute hrefs are left alone —
    the old 'https://247sports.comhttps://...' doubling came from prepending
    the base to an already-absolute href."""
    slug, pid = m.group(1), m.group(2)
    is_placeholder = (pid in PLACEHOLDER_IDS or slug == '' or set(slug) <= {'-'})
    href = a['href']
    if href.startswith('http://') or href.startswith('https://'):
        full_url = href.rstrip('/') + '/'
    else:
        full_url = f"https://247sports.com{href.rstrip('/')}/"
    return {
        'name': a.get_text(strip=True),
        '247_id': None if is_placeholder else pid,
        'profile_url': None if is_placeholder else full_url,
    }


def _find_row_anchor(el):
    """The player anchor INSIDE this row. Scoping the anchor to its own row is
    what guarantees name/ID alignment — the old global-anchor sliding window
    could shift every field on the page."""
    for a in el.find_all('a', href=True):
        m = PLAYER_URL_RE.search(a['href'])
        if m and a.get_text(strip=True):
            return _anchor_fields(a, m)
    return None


def _table_rows(soup):
    """Classic <table> roster layout. Returns [(row_el, [cell_texts]), ...]."""
    for table in soup.find_all('table'):
        trs = table.find_all('tr')
        if len(trs) < 2:
            continue
        header_cells = [c.get_text(strip=True).lower()
                        for c in trs[0].find_all(['th', 'td'])]
        header_text = ' '.join(header_cells)
        if not ('jersey' in header_text or 'pos' in header_text
                or 'height' in header_text):
            continue
        out = []
        for tr in trs[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
            if len(cells) < 6:
                continue
            out.append((tr, cells))
        if out:
            return out
    return []


def _list_rows(soup):
    """Fallback for a non-<table> roster layout (247 has moved other pages to
    ul/li). Anchor-first: walk up from each player link to its repeating row
    container, then keep the largest sibling group."""
    groups = {}
    for a in soup.find_all('a', href=True):
        m = PLAYER_URL_RE.search(a['href'])
        if not m or not a.get_text(strip=True):
            continue
        node, row = a, None
        for _ in range(6):
            node = node.parent
            if node is None:
                break
            if node.name in ROW_CONTAINER_TAGS:
                row = node
                break
        if row is None or row.parent is None:
            continue
        groups.setdefault(id(row.parent), []).append(row)

    if not groups:
        return []
    best = max(groups.values(), key=len)
    if len(best) < MIN_LIST_ROWS:
        return []

    out = []
    seen = set()
    for row in best:
        if id(row) in seen:
            continue
        seen.add(id(row))
        cells = [t.strip() for t in row.stripped_strings if t.strip()]
        out.append((row, cells))
    return out


def _fields_positional(cells):
    """Column order verified against the 2018-2025 table layout."""
    jersey, pos, height, weight, yr, age, hs, rating = (cells + [''] * 8)[:8]
    return {'jersey': jersey, 'position': pos, 'height': height,
            'weight': weight, 'class_yr': yr, 'age': age,
            'high_school': hs, 'rating': rating}


def _fields_by_pattern(cells):
    """Assign fields by shape, not by index. Used for the non-table fallback,
    where column order is unknown. Anything that does not match a pattern is
    left BLANK rather than guessed into the wrong column."""
    out = {'jersey': '', 'position': '', 'height': '', 'weight': '',
           'class_yr': '', 'age': '', 'high_school': '', 'rating': ''}
    for c in cells:
        t = c.strip()
        if not t:
            continue
        if not out['height'] and HEIGHT_RE.match(t):
            out['height'] = t
            continue
        if not out['rating'] and RATING_RE.match(t):
            out['rating'] = t
            continue
        if not out['class_yr'] and t.upper().replace('.', '') in CLASS_YR_TOKENS:
            out['class_yr'] = t.upper()
            continue
        if not out['weight'] and WEIGHT_RE.match(t) and 140 <= int(t) <= 420:
            out['weight'] = t
            continue
        if not out['jersey'] and JERSEY_RE.match(t):
            out['jersey'] = JERSEY_RE.match(t).group(1)
            continue
        if not out['position'] and 1 <= len(t) <= 3 and t.isalpha() and t.isupper():
            out['position'] = t
            continue
    return out


def _normalize_height(height):
    """Store 5'8\" not 5-8. Bare 5-8 is what Excel turns into '8-May'."""
    m = re.match(r"^\s*(\d{1,2})-(\d{1,2})\s*$", height or '')
    if m:
        return f"{m.group(1)}'{m.group(2)}\""
    return height or ''


def sanity_flags(rows):
    """Catch the failure that corrupted the April output: a whole team-season
    where one column collapses to a single repeated value (every player #31/DB).
    Positional parsing produces exactly this when the table shifts."""
    flags = []
    if len(rows) < 20:
        return flags
    for f in ('jersey', 'position', 'height', 'weight', 'class_yr'):
        vals = [str(r.get(f, '') or '').strip() for r in rows]
        nonblank = [v for v in vals if v]
        if len(nonblank) < 20:
            continue
        top = max(set(nonblank), key=nonblank.count)
        share = nonblank.count(top) / len(nonblank)
        if share > 0.6:
            flags.append(f"{f}={top!r} on {share:.0%} of rows")
    return flags


def parse_roster_html(html, team, season):
    """Extract roster rows. Table layout first (verified for 2018-2025), then a
    non-table fallback that captures identity fields with confidence and leaves
    unmatched columns blank."""
    soup = BeautifulSoup(html, 'lxml')

    table_rows = _table_rows(soup)
    if table_rows:
        row_pairs, mode = table_rows, 'table'
    else:
        row_pairs, mode = _list_rows(soup), 'list'
    if not row_pairs:
        return []

    # Global-anchor fallback only for rows with no anchor of their own.
    global_anchors = []
    for a in soup.find_all('a', href=True):
        m = PLAYER_URL_RE.search(a['href'])
        if m and a.get_text(strip=True):
            global_anchors.append(_anchor_fields(a, m))

    rows = []
    unanchored = 0
    for idx, (row_el, cells) in enumerate(row_pairs):
        anchor = _find_row_anchor(row_el)
        if anchor is None:
            unanchored += 1
            if idx < len(global_anchors):
                anchor = global_anchors[idx]
            else:
                continue

        f = _fields_positional(cells) if mode == 'table' else _fields_by_pattern(cells)

        composite_from_roster = ''
        if f['rating']:
            m_rating = re.match(r'^\s*(\d*\.\d+)', f['rating'])
            if m_rating:
                composite_from_roster = m_rating.group(1)

        row = {
            '247_id': anchor['247_id'],
            'player_name': anchor['name'],
            'team': team,
            'season': season,
            'jersey': f['jersey'],
            'position': f['position'],
            'height': _normalize_height(f['height']),
            'weight': f['weight'],
            'class_yr': f['class_yr'],
            'age': f['age'],
            'high_school': f['high_school'],
            'profile_url': anchor['profile_url'] or '',
            'scrape_ts': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
        }
        for fld in HS_FIELDS + TRANSFER_FIELDS + STATUS_FIELDS:
            row[fld] = ''
        row['hs_composite_rating'] = composite_from_roster
        rows.append(row)

    if rows and unanchored:
        print(f"  NOTE: {team} {season} — {unanchored}/{len(row_pairs)} rows had "
              f"no in-row player link (fell back to page order)")
    if mode == 'list' and rows:
        print(f"  NOTE: {team} {season} — parsed via NON-TABLE fallback "
              f"({len(rows)} rows); check debug dump before trusting columns")
    return rows


# ---------- Section parsing ----------
def _parse_section_common(section, is_juco):
    out = {'rating': '', 'year': None, 'stars': ''}
    rating_block = (section.select_one('.rank-block')
                    or section.select_one('.score')
                    or section.select_one('.rating'))
    if rating_block:
        year_tag = rating_block.select_one('.rank-year')
        if year_tag:
            m_year = re.search(r'\((\d{4})\)', year_tag.get_text(' ', strip=True))
            if m_year:
                out['year'] = int(m_year.group(1))
        rating_text = rating_block.get_text(' ', strip=True)
        if not _is_na(rating_text):
            m_rating = re.match(r'^\s*(\d+(?:\.\d+)?)', rating_text)
            if m_rating:
                out['rating'] = m_rating.group(1)
        if out['year'] is None:
            m_year2 = re.search(r'\((\d{4})\)', rating_text)
            if m_year2:
                out['year'] = int(m_year2.group(1))

    if is_juco:
        out['stars'] = 'JUCO'
    else:
        stars = section.select('span.icon-starsolid.yellow, i.icon-starsolid.yellow')
        if stars:
            out['stars'] = str(min(len(stars), 5))
    return out


def _parse_transfer_section(section):
    """Transfer section: position rank row uses positionKey= in href."""
    out = {'kind': 'Transfer'}
    out.update(_parse_section_common(section, is_juco=False))
    out.update({'overall_rank': '', 'national_rank': '',
                'position_rank': '', 'position': ''})

    ranks_list = section.select_one('ul.ranks-list')
    li_iter = ranks_list.select('li') if ranks_list else section.select('li')

    for li in li_iter:
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
                out['overall_rank'] = _parse_rank(value)
        elif 'positionKey=' in href:
            if not out['position']:
                out['position'] = label
                if not value_is_na:
                    out['position_rank'] = _parse_rank(value)
        elif not out['position']:
            out['position'] = label
            if not value_is_na:
                out['position_rank'] = _parse_rank(value)
    return out


def _parse_prospect_section(section, is_juco):
    """Prospect section: position rank uses Position=, state rows use State=
    (skip), national rank uses InstitutionGroup=HighSchool with no Position=."""
    out = {'kind': 'JUCO' if is_juco else '247Sports'}
    out.update(_parse_section_common(section, is_juco=is_juco))
    out.update({'overall_rank': '', 'national_rank': '',
                'position_rank': '', 'position': ''})

    ranks_list = section.select_one('ul.ranks-list')
    li_iter = ranks_list.select('li') if ranks_list else section.select('li')

    for li in li_iter:
        bold = li.find('b')
        strong = li.find('strong')
        if not bold or not strong:
            continue
        label = bold.get_text(strip=True).upper()
        value = strong.get_text(strip=True)
        value_is_na = _is_na(value)
        link = li.find('a')
        href = (link.get('href', '') if link else '')

        if 'State=' in href or 'state=' in href:
            continue
        if 'Position=' in href:
            if not out['position']:
                out['position'] = label
                if not value_is_na:
                    out['position_rank'] = _parse_rank(value)
            continue
        if 'InstitutionGroup=HighSchool' in href:
            if not value_is_na:
                out['national_rank'] = _parse_rank(value)
            continue
        if 'OVR' in label or 'OVERALL' in label:
            if not value_is_na:
                out['overall_rank'] = _parse_rank(value)
        elif 'NATL' in label or 'NATIONAL' in label:
            if not value_is_na:
                out['national_rank'] = _parse_rank(value)
        elif not out['position']:
            out['position'] = label
            if not value_is_na:
                out['position_rank'] = _parse_rank(value)
    return out


def _prospect_event_is_real(ev):
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
    """Parse origin team, ALL transfer events, and a prospect event from a
    profile. We parse the /college-{id}/ view directly (no navigation), which
    holds both transfer and prospect sections."""
    soup = BeautifulSoup(html, 'lxml')
    result = {
        'origin_team': '',
        'transfer_events': [],
        'prospect_event': None,
        'section_titles': [],
    }

    # Origin team — best effort. Primary: team-info-section header h2.
    team_header = soup.select_one('.team-info-section header h2')
    if team_header:
        result['origin_team'] = team_header.get_text(strip=True)

    juco_event = None
    primary_247_event = None
    composite_event = None

    sections = soup.select(
        'section.rankings, section.rankings-section, div.ranking-section'
    )

    for section in sections:
        title_tag = (section.select_one('.rankings-header h3')
                     or section.select_one('h3.title')
                     or section.select_one('h3'))
        if not title_tag:
            continue
        title = title_tag.get_text(strip=True)
        result['section_titles'].append(title)

        title_upper = title.upper()
        is_juco = 'JUCO' in title_upper

        if 'TRANSFER' in title_upper:
            ev = _parse_transfer_section(section)
            result['transfer_events'].append(ev)
        elif is_juco:
            ev = _parse_prospect_section(section, is_juco=True)
            ev['kind'] = 'JUCO'
            if juco_event is None:
                juco_event = ev
        elif 'COMPOSITE' in title_upper and '247SPORTS' in title_upper:
            ev = _parse_prospect_section(section, is_juco=False)
            ev['kind'] = '247Sports Composite'
            if composite_event is None:
                composite_event = ev
        elif '247SPORTS' in title_upper:
            ev = _parse_prospect_section(section, is_juco=False)
            ev['kind'] = '247Sports'
            if primary_247_event is None:
                primary_247_event = ev

    if juco_event:
        result['prospect_event'] = juco_event
    elif _prospect_event_is_real(primary_247_event):
        result['prospect_event'] = primary_247_event
    elif _prospect_event_is_real(composite_event):
        result['prospect_event'] = composite_event
    elif primary_247_event:
        result['prospect_event'] = primary_247_event
    elif composite_event:
        result['prospect_event'] = composite_event

    return result


def pick_transfer_event(profile, season):
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
    """Fetch & parse a single player profile.

    NO navigation hops — we parse the /college-{id}/ view directly, which
    contains both the transfer and prospect sections.
    """
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
                                wait_until='commit')
                await page.wait_for_timeout(PROFILE_POST_NAV_WAIT_MS)
                try:
                    await page.wait_for_selector(
                        '.name, h1.name',
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
                        'origin_team': '', 'transfer_events': [],
                        'prospect_event': None, 'section_titles': [],
                        'fetch_status': 'failed', 'fetched_url': url,
                    }
        return {
            'origin_team': '', 'transfer_events': [],
            'prospect_event': None, 'section_titles': [],
            'fetch_status': 'failed', 'fetched_url': url,
        }


def _apply_juco_suffix(value, kind):
    if not value or _is_na(value):
        return value
    if kind == 'JUCO' and '(JUCO)' not in str(value):
        return f"{value} (JUCO)"
    return value


def apply_profile_to_row(row, profile, season):
    """Write HS and transfer fields. hs_composite_rating stays from roster
    table. transfer_* populated ONLY when a transfer event exists."""
    if profile.get('fetch_status') == 'failed':
        row['profile_scraped'] = 'failed'
        return

    pe = profile.get('prospect_event')
    if pe:
        kind = pe.get('kind') or '247Sports'
        row['hs_section_kind'] = kind
        row['hs_scout_rating'] = pe.get('rating', '') or ''
        row['hs_position'] = pe.get('position', '') or ''
        row['hs_stars'] = pe.get('stars', '') or ''
        year = pe.get('year')
        row['hs_class_year'] = str(year) if year else ''
        row['hs_national_rank'] = _apply_juco_suffix(pe.get('national_rank', '') or '', kind)
        row['hs_position_rank'] = _apply_juco_suffix(pe.get('position_rank', '') or '', kind)

    event = pick_transfer_event(profile, season)
    if event is None:
        row['profile_scraped'] = 'ok_no_transfer'
        return

    # A transfer event exists → this player transferred. Populate transfer_*.
    row['profile_scraped'] = 'ok'
    row['transfer_rating']         = event.get('rating', '') or ''
    row['transfer_overall_rank']   = event.get('overall_rank', '') or ''
    row['transfer_position_rank']  = event.get('position_rank', '') or ''
    row['transfer_position']       = event.get('position', '') or ''
    row['transfer_class_year']     = str(event.get('year', '') or '')
    row['transfer_stars']          = event.get('stars', '') or ''

    # Origin team — best effort from profile, but only if it differs from the
    # current roster team (origin should not equal where they are now).
    origin = profile.get('origin_team', '') or ''
    current_team = row.get('team', '') or ''
    if origin and origin.strip().lower() != current_team.strip().lower():
        row['transfer_origin_team'] = origin
    else:
        row['transfer_origin_team'] = ''


async def enrich_with_profiles(rows, context, profile_cache, concurrency, season):
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
_debug_dumps = 0


async def dump_debug(page, team, season, requested_url, reason, html=None):
    """Write the evidence needed to fix a zero-row failure without guessing:
    final URL after redirects, page title, a selector census, the full HTML and
    a screenshot. Capped so a systemic failure doesn't produce a huge artifact."""
    global _debug_dumps
    if _debug_dumps >= MAX_DEBUG_DUMPS:
        return
    _debug_dumps += 1
    DEBUG_DIR.mkdir(exist_ok=True)
    safe = team.replace(' ', '_').replace('/', '_').replace('(', '').replace(')', '')
    stem = f"{season}_{safe}_{reason}"

    final_url, title = '', ''
    try:
        final_url = page.url
    except Exception:
        pass
    try:
        title = await page.title()
    except Exception:
        pass

    if html is None:
        try:
            html = await page.content()
        except Exception:
            html = ''
    try:
        (DEBUG_DIR / f"{stem}.html").write_text(html or '', encoding='utf-8')
    except Exception as e:
        print(f"  WARN: could not write debug html: {e}")
    try:
        await page.screenshot(path=str(DEBUG_DIR / f"{stem}.png"), full_page=True)
    except Exception as e:
        print(f"  WARN: could not write debug screenshot: {e}")

    census = {}
    for sel in ('table', 'table tr', 'a[href*="/player/"]', 'li',
                '[class*="roster" i]', '[class*="player" i]'):
        try:
            census[sel] = await page.locator(sel).count()
        except Exception:
            census[sel] = -1

    info = {
        'team': team, 'season': season, 'reason': reason,
        'requested_url': requested_url, 'final_url': final_url,
        'redirected': bool(final_url and final_url.rstrip('/') != requested_url.rstrip('/')),
        'page_title': title, 'html_bytes': len(html or ''),
        'selector_counts': census,
        'captured_at': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
    }
    try:
        (DEBUG_DIR / f"{stem}.json").write_text(json.dumps(info, indent=2), encoding='utf-8')
    except Exception:
        pass
    n_tables = census.get('table')
    n_players = census.get('a[href*="/player/"]')
    print(f"  DEBUG → debug/{stem}.[html|png|json]  final_url={final_url}  "
          f"tables={n_tables}  player_links={n_players}")


async def scrape_team_season(page, team, season, verbose=True, allow_empty=False):
    """Returns (rows, reason). rows is None on failure, [] only when the page
    genuinely rendered with no players AND allow_empty is set.

    The 2026 break was here: wait_for_selector('table') timed out at 10s and the
    function returned [], which the caller recorded as 'OK (0 rows)'. A zero-row
    parse is now a failure with a debug dump attached."""
    last_reason = 'unknown'
    for cand_i, url in enumerate(team_url_candidates(team, season)):
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=NAV_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            last_reason = 'nav_timeout'
            if verbose:
                print(f"  TIMEOUT goto {team} {season} ({url})")
            continue
        except Exception as e:
            last_reason = f'nav_error_{type(e).__name__}'
            if verbose:
                print(f"  ERROR goto {team} {season}: {type(e).__name__}: {e}")
            continue

        try:
            await page.wait_for_selector(ROSTER_READY_SELECTOR,
                                         timeout=ROSTER_SELECTOR_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            last_reason = 'no_roster_markup'
            if verbose:
                print(f"  NO ROSTER MARKUP for {team} {season} ({url})")
            await dump_debug(page, team, season, url, f'{last_reason}_c{cand_i}')
            continue

        # Let a lazily-rendered roster settle before reading the DOM.
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(LAZY_SETTLE_MS)
        except Exception:
            pass

        html = await page.content()
        rows = parse_roster_html(html, team, season)
        if rows:
            if cand_i > 0:
                print(f"  NOTE: {team} {season} used FALLBACK url #{cand_i} "
                      f"({url}) — not year-scoped, verify the season is right")
            flags = sanity_flags(rows)
            if flags:
                print(f"  ⚠ SUSPECT COLUMNS {team} {season}: {'; '.join(flags)}")
            return rows, 'ok'

        last_reason = 'parsed_zero_rows'
        await dump_debug(page, team, season, url, f'{last_reason}_c{cand_i}', html=html)

    if allow_empty and last_reason == 'parsed_zero_rows':
        return [], 'empty_allowed'
    return None, last_reason


# ---------- Orchestrator ----------
async def run(seasons, teams, output, skip_existing=True,
              skip_profiles=False, profile_concurrency=DEFAULT_PROFILE_CONCURRENCY,
              allow_empty=False):
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
    successes = 0
    consecutive_failures = 0
    failures = []
    empties = []
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
                try:
                    head = pd.read_csv(ckpt, nrows=1)
                    has_new_schema = (
                        'hs_class_year' in head.columns
                        and 'hs_composite_rating' in head.columns
                        and 'hs_scout_rating' in head.columns
                        and 'transfer_origin_team' in head.columns
                        and 'transfer_destination_team' not in head.columns
                        and 'draft_year' not in head.columns
                    )
                    sample = pd.read_csv(
                        ckpt,
                        usecols=lambda c: c in ('profile_url', 'profile_scraped',
                                                'height', 'hs_section_kind',
                                                'hs_scout_rating'),
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
                    # 0-row checkpoint that was a roster failure → redo
                    empty_failed = (len(sample) == 0)
                    poisoned = (bad_url_share > 0.1
                                or fail_share > 0.5
                                or height_corrupt
                                or empty_failed)
                except Exception:
                    has_new_schema = False
                    poisoned = False
                if poisoned:
                    print(f"[{i+1}/{len(tasks)}] REDO {team} {season} (checkpoint poisoned)")
                elif has_new_schema or skip_profiles:
                    skipped += 1
                    if i % 20 == 0:
                        print(f"[{i+1}/{len(tasks)}] SKIP {team} {season} (cached)")
                    continue
                else:
                    print(f"[{i+1}/{len(tasks)}] BACKFILL {team} {season} (schema mismatch)")

            if i > 0 and i % 30 == 0:
                await context.close()
                context = await browser.new_context(
                    user_agent=random.choice(USER_AGENTS),
                    viewport={'width': 1280, 'height': 900},
                )
                page = await context.new_page()

            rows = None
            reason = 'unknown'
            for attempt in range(MAX_RETRIES_PER_PAGE):
                try:
                    rows, reason = await scrape_team_season(
                        page, team, season, verbose=True, allow_empty=allow_empty)
                    if rows is not None:
                        break
                except Exception as e:
                    reason = f'exception_{type(e).__name__}'
                    print(f"  retry {attempt+1}: {type(e).__name__}: {e}")
                await asyncio.sleep(5 + attempt * 3)

            status = ''
            if rows is None:
                consecutive_failures += 1
                failures.append((team, season, reason))
                status = f'FAIL ({reason})'
                with open(ckpt, 'w', newline='') as f:
                    f.write('# SCRAPE FAILED — delete this file to retry\n')
                # Cool-off + fresh context: transient 247 block recovery
                print(f"  roster block — cooling off {ROSTER_BLOCK_BACKOFF_S}s "
                      f"& rotating context ({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES})")
                await asyncio.sleep(ROSTER_BLOCK_BACKOFF_S)
                try:
                    await context.close()
                except Exception:
                    pass
                context = await browser.new_context(
                    user_agent=random.choice(USER_AGENTS),
                    viewport={'width': 1280, 'height': 900},
                )
                page = await context.new_page()
            else:
                consecutive_failures = 0
                if rows:
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
                    successes += 1
                    status = f'OK ({len(rows)} rows)'
                else:
                    # Only reachable with --allow-empty; still not a silent pass.
                    pd.DataFrame(columns=ALL_OUTPUT_COLS).to_csv(ckpt, index=False)
                    empties.append((team, season))
                    status = 'EMPTY (0 rows — allowed by flag)'

            completed += 1
            elapsed = time.time() - started_at
            rate = completed / elapsed if elapsed > 0 else 0
            eta_s = (len(tasks) - skipped - completed) / rate if rate > 0 else 0
            cache_note = (f"  cache={len(profile_cache):,}"
                          f" (+{new_profiles_this_run} new)") if not skip_profiles else ""
            print(f"[{i+1}/{len(tasks)}] {team:24s} {season}  {status}   "
                  f"elapsed={elapsed/60:.1f}m  eta={eta_s/60:.0f}m{cache_note}")

            if len(failures) >= EARLY_ABORT_AFTER and successes == 0:
                print(f"\n⚠  ABORTING EARLY: first {len(failures)} team-seasons all "
                      f"failed and none succeeded — this is systemic, not transient.\n"
                      f"   Reasons: {sorted(set(r for _, _, r in failures))}\n"
                      f"   Check the `debug` artifact: final_url tells you if 247 "
                      f"redirected, selector_counts tells you if the markup changed.")
                break

            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                print(f"\n⚠  ABORTING: {MAX_CONSECUTIVE_FAILURES} consecutive failures.")
                break

            await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        await browser.close()

    if not skip_profiles:
        flush_profile_cache(profile_cache)

    # ---------- Consolidate ----------
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

        print(f"\n=== Post-run validation ===")
        print(f"Distinct 247 IDs:    {full['247_id'].nunique():,}")
        print(f"Rows with 247 ID:    {full['247_id'].notna().sum():,}  "
              f"({full['247_id'].notna().mean():.1%})")
        if not skip_profiles:
            for f in (HS_FIELDS + TRANSFER_FIELDS):
                filled = full[f].astype(str).str.strip().replace('nan', '').ne('').sum()
                print(f"  {f:32s} filled: {filled:6,}  ({filled/len(full):.1%})")
            transfers = (full['profile_scraped'] == 'ok').sum()
            print(f"\nRows flagged as transfers (profile_scraped='ok'): {transfers:,}")
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

    # ---------- Failure report + exit code ----------
    attempted = successes + len(failures) + len(empties)
    if failures:
        print(f"\n=== FAILED TEAM-SEASONS ({len(failures)}/{attempted}) ===")
        by_reason = {}
        for team, season, reason in failures:
            by_reason.setdefault(reason, []).append(f"{team} {season}")
        for reason, items in sorted(by_reason.items()):
            print(f"  {reason}: {len(items)}")
            for it in items[:10]:
                print(f"      {it}")
            if len(items) > 10:
                print(f"      ... +{len(items) - 10} more")
        if DEBUG_DIR.exists():
            print(f"\n  Debug captures written to {DEBUG_DIR}/ "
                  f"({len(list(DEBUG_DIR.glob('*.json')))} team-seasons). "
                  f"Read the .json first: final_url + selector_counts.")

    if not dfs:
        print("\nFAILING: zero rows collected. This is not a successful run.")
        sys.exit(2)
    if attempted and len(failures) / attempted > 0.10:
        print(f"\nFAILING: {len(failures)/attempted:.0%} of team-seasons failed "
              f"(threshold 10%).")
        sys.exit(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--seasons', nargs='+', type=int, required=True)
    ap.add_argument('--teams', nargs='+', default=None)
    ap.add_argument('--output', default='roster_full.csv')
    ap.add_argument('--force', action='store_true',
                    help='Ignore checkpoints, re-scrape all')
    ap.add_argument('--skip-profiles', action='store_true')
    ap.add_argument('--allow-empty', action='store_true',
                    help='Treat a rendered-but-playerless roster page as an '
                         'empty team-season instead of a failure')
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
        allow_empty=args.allow_empty,
    ))


if __name__ == '__main__':
    main()
