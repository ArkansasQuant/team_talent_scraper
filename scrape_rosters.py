"""
Scrape active rosters for all FBS teams across seasons from 247sports,
then enrich each roster row by visiting the player profile to capture
HS recruiting metadata, transfer-portal metadata, and NFL draft info.

Design principles (from scraping playbook):
  - Playwright + domcontentloaded (NOT networkidle)
  - Randomized delays + user-agent rotation
  - NEVER break-on-exception in loops — use continue + failure counter
  - Per-team-season checkpointing for crash recovery
  - GLOBAL profile cache keyed by (247_id, team_canonical) — different
    team views of the same player are cached separately. Profile content
    on 247 depends on the /college-{id}/ URL suffix, so the same player
    on Ohio State 2019 vs Alabama 2021 needs to be fetched twice and
    cached as two entries. Previously the cache was keyed by 247_id alone,
    causing alphabetically-first team views to poison all later team rows.
  - Cache flushed to disk after every team-season so a mid-run crash
    can resume without re-scraping any profiles
  - Profile fetches run concurrently (asyncio.Semaphore)
  - Post-run verification (row counts, ID format, null-ratio sanity)

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

from team_urls import TEAM_URLS, team_url, is_fbs_in_year, all_teams

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
PROFILE_NAV_TIMEOUT_MS = 30000
PROFILE_SELECTOR_TIMEOUT_MS = 5000
MAX_CONSECUTIVE_FAILURES = 8
MAX_RETRIES_PER_PAGE = 3
MAX_PROFILE_RETRIES = 2
DEFAULT_PROFILE_CONCURRENCY = 6
PROFILE_DELAY_MIN = 0.3
PROFILE_DELAY_MAX = 1.0

# Bump this whenever the cache schema changes so old caches get thrown out.
# Schema 5 changes:
#   - Cache key is now "{player_id}:{team_canonical}" (was just "{player_id}")
#   - profile dict now includes prospect_event and draft_info fields
PROFILE_CACHE_SCHEMA = 5

CHECKPOINT_DIR = Path("checkpoints")
PROFILE_CACHE_FILE = CHECKPOINT_DIR / "profiles_cache.json"

# Regex to extract 247 ID from player URL, e.g. /player/carlton-martial-91227/
PLAYER_URL_RE = re.compile(r'/player/([^/]*?)-(\d+)/?$')

# Columns added by the profile enrichment step
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

# HS / recruiting fields (NEW — section: 247Sports or JUCO)
HS_FIELDS = [
    'hs_class_year',           # (YYYY) from the prospect rank-block
    'hs_composite_rating',     # numeric 247 composite rating, e.g. "95"
    'hs_national_rank',        # national rank — JUCO ranks suffixed " (JUCO)"
    'hs_position_rank',        # position rank — JUCO ranks suffixed " (JUCO)"
    'hs_position',             # position from prospect section
    'hs_stars',                # 1-5 or 'JUCO'
    'hs_section_kind',         # '247Sports' or 'JUCO'
]

# NFL draft fields (NEW)
DRAFT_FIELDS = [
    'draft_year',
    'draft_round',
    'draft_pick',
    'draft_team',
]

# profile_scraped is one of:
#   'ok'              — fetched + parsed; matching transfer event applied
#   'ok_no_transfer'  — fetched + parsed but no transfer event applies for this season
#   'failed'          — fetch error after all retries
#   'no_url'          — row had no profile URL (placeholder/walk-on)
#   'skipped'         — enrichment was disabled
STATUS_FIELDS = ['profile_scraped']

ALL_OUTPUT_COLS = [
    '247_id', 'player_name', 'team', 'season', 'jersey', 'position',
    'height', 'weight', 'class_yr', 'age', 'high_school',
    'profile_url', 'scrape_ts',
] + HS_FIELDS + TRANSFER_FIELDS + DRAFT_FIELDS + STATUS_FIELDS


# ---------- Roster page extraction ----------
def parse_roster_html(html, team, season):
    """
    Extract roster rows from a team roster page.

    Returns a list of dicts, one per roster entry. Combines the player-link
    list (which has names + 247 IDs) with the jersey table (which has
    position, height, weight, class year, HS, rating) by row order.
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
            is_placeholder = (
                pid == '46120272'
                or slug == ''
                or set(slug) <= {'-'}
            )
            name = a.get_text(strip=True)
            if name:
                # 247 returns absolute hrefs (https://247sports.com/...) and
                # relative ones. Detect to avoid double-prefixing.
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
        break

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
        # Convert "6-2" → 6'2" so Excel doesn't auto-date as 2-Jun
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
            # NOTE: roster-table "Rating" column is now derived from profile
            # data; we don't capture it from the roster table anymore because
            # the profile gives us a richer breakdown (HS rating, transfer
            # rating, both with national/position ranks).
            'profile_url': anchor['profile_url'] or '',
            'scrape_ts': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
        }
        # Initialize all enrichment fields as empty
        for f in HS_FIELDS + TRANSFER_FIELDS + DRAFT_FIELDS + STATUS_FIELDS:
            row[f] = ''
        rows.append(row)
    return rows


# ---------- Player profile extraction ----------
def _parse_one_section(section, is_juco_title):
    """Parse a single rankings-section into a normalized event dict.

    Captures rating, year, overall_rank, national_rank, position_rank,
    position, stars. The national_rank field is populated separately from
    position_rank so prospect sections (which have NATL labels) can use it,
    while transfer sections (which don't) leave it blank.
    """
    out = {
        'kind': '', 'rating': '', 'year': None,
        'overall_rank': '', 'national_rank': '',
        'position_rank': '', 'position': '', 'stars': '',
    }

    rating_block = section.select_one('.rank-block')
    if rating_block:
        rating_text = rating_block.get_text(' ', strip=True)
        m_rating = re.match(r'^\s*(\d+(?:\.\d+)?)', rating_text)
        if m_rating:
            out['rating'] = m_rating.group(1)
        m_year = re.search(r'\((\d{4})\)', rating_text)
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
            # National rank — populated for prospect/JUCO sections
            out['national_rank'] = value
        elif 'State=' in href:
            # State rank — skip
            continue
        elif not out['position_rank']:
            # First remaining rank = position rank
            out['position_rank'] = value
            out['position'] = label
    return out


def _parse_draft_info(soup, full_text):
    """Extract NFL draft year, round, pick, and team.

    247 displays draft info as a banner near the top of the profile when a
    player has been drafted. Format examples seen:
      "Drafted by Detroit Lions — Round 1, Pick 12 (2022 NFL Draft)"
      "12th overall pick by Detroit Lions in the 2022 NFL Draft (Round 1)"
      Banner element classes include .draft-banner, .draft-info, .player-draft
    """
    out = {'draft_year': '', 'draft_round': '', 'draft_pick': '', 'draft_team': ''}

    # Strategy 1: look for explicit draft banner / section
    banner = (soup.select_one('.draft-banner')
              or soup.select_one('.player-draft')
              or soup.select_one('[class*="draft-info"]')
              or soup.select_one('[class*="draft-pick"]'))
    text = ''
    if banner:
        text = banner.get_text(' ', strip=True)
    else:
        # Strategy 2: scan the whole page text for "NFL Draft" mentions
        if 'NFL Draft' in full_text or 'NFL draft' in full_text:
            # Take a window of text around the FIRST NFL Draft mention
            idx = full_text.lower().find('nfl draft')
            start = max(0, idx - 200)
            end = min(len(full_text), idx + 200)
            text = full_text[start:end]

    if not text:
        return out

    # Year — look for 4-digit year near "NFL Draft"
    m_year = re.search(r'(\d{4})\s+NFL\s+Draft', text, re.IGNORECASE)
    if m_year:
        out['draft_year'] = m_year.group(1)

    # Round
    m_round = re.search(r'Round\s+(\d+)', text, re.IGNORECASE)
    if m_round:
        out['draft_round'] = m_round.group(1)

    # Pick — overall pick number
    m_pick = re.search(r'(?:Pick|Overall)\s+#?(\d+)', text, re.IGNORECASE)
    if not m_pick:
        m_pick = re.search(r'#?(\d+)(?:st|nd|rd|th)\s+overall', text, re.IGNORECASE)
    if m_pick:
        out['draft_pick'] = m_pick.group(1)

    # Team — "by [Team Name]" or "Drafted by [Team Name]"
    m_team = re.search(
        r'(?:Selected|Drafted|Picked)\s+by\s+(?:the\s+)?([A-Z][\w\.\' ]+?)'
        r'\s+(?:in|—|–|-|\(|with|at)',
        text,
    )
    if m_team:
        out['draft_team'] = m_team.group(1).strip()

    return out


def parse_player_profile(html):
    """
    Pull origin/destination teams, ALL transfer events, the prospect event,
    and NFL draft info from a profile.

    Returns:
      {
        'origin_team': str,
        'destination_team': str,
        'transfer_events': [...],
        'prospect_event': {...} | None,   # 247Sports HS recruiting (prefers JUCO if present)
        'draft_info': {draft_year, draft_round, draft_pick, draft_team},
        'section_titles': [...],
      }
    """
    soup = BeautifulSoup(html, 'lxml')
    result = {
        'origin_team': '',
        'destination_team': '',
        'transfer_events': [],
        'prospect_event': None,
        'draft_info': {'draft_year': '', 'draft_round': '',
                       'draft_pick': '', 'draft_team': ''},
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

    # Iterate ALL rankings sections. Classify by title content.
    juco_event = None
    sevenforty_event = None
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
        elif is_juco:
            ev = _parse_one_section(section, is_juco_title=True)
            ev['kind'] = 'JUCO'
            if juco_event is None:
                juco_event = ev
        elif title == '247Sports':
            ev = _parse_one_section(section, is_juco_title=False)
            ev['kind'] = '247Sports'
            if sevenforty_event is None:
                sevenforty_event = ev

    # Prefer JUCO event if present (more specific data), else 247Sports
    result['prospect_event'] = juco_event if juco_event else sevenforty_event

    # NFL draft info
    full_text = soup.get_text(' ', strip=True)
    result['draft_info'] = _parse_draft_info(soup, full_text)

    return result


def pick_transfer_event(profile, season):
    """Choose the transfer event for this roster season.

    Prefers events at-or-before the season (most recent winning). Falls back
    to events with year=None if no dated events match — better to surface
    SOME transfer data than to silently drop the row.
    """
    events = profile.get('transfer_events') or []
    if not events:
        return None
    # Dated events at-or-before this season
    dated = [e for e in events if e.get('year') and e['year'] <= season]
    if dated:
        dated.sort(key=lambda e: e.get('year') or 0)
        return dated[-1]
    # Fallback: undated events — use the LAST one (page order = most-recent-first
    # on most 247 layouts; if it's the only event we'd rather show it than drop)
    undated = [e for e in events if not e.get('year')]
    if undated:
        return undated[-1]
    # All remaining events are in the future relative to this season — don't pollute
    return None


# ---------- Profile cache ----------
def _cache_key(player_id, team_canonical):
    """Cache key combines player_id AND team. Different team views of the
    same player are different cache entries — 247's profile content depends
    on the /college-{id}/ in the URL, so caching by player_id alone causes
    the first-team-wins poisoning bug.
    """
    return f"{player_id}:{team_canonical or ''}"


def load_profile_cache():
    """Load cached profile lookups."""
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
        print(f"  Loaded {len(cache):,} cached profile views from {PROFILE_CACHE_FILE}")
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
    Required for the team-info-section, commit-banner, AND Transfer section
    to render correctly on the team-specific view.
    """
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
                        'draft_info': {'draft_year': '', 'draft_round': '',
                                       'draft_pick': '', 'draft_team': ''},
                        'section_titles': [],
                        'fetch_status': 'failed', 'fetched_url': url,
                    }
        return {
            'origin_team': '', 'destination_team': '',
            'transfer_events': [], 'prospect_event': None,
            'draft_info': {'draft_year': '', 'draft_round': '',
                           'draft_pick': '', 'draft_team': ''},
            'section_titles': [],
            'fetch_status': 'failed', 'fetched_url': url,
        }


def _apply_juco_suffix(value, kind):
    """Return value with ' (JUCO)' suffix when section kind is JUCO.
    Skips empty values and values that already contain '(JUCO)'.
    """
    if not value:
        return value
    if kind == 'JUCO' and '(JUCO)' not in str(value):
        return f"{value} (JUCO)"
    return value


def apply_profile_to_row(row, profile, season):
    """Write HS, transfer, and draft fields from profile into the row."""

    # Failed fetch: short-circuit with status, no fields populated
    if profile.get('fetch_status') == 'failed':
        row['profile_scraped'] = 'failed'
        return

    # ----- HS / Recruiting (always written if prospect_event exists) -----
    pe = profile.get('prospect_event')
    if pe:
        kind = pe.get('kind') or '247Sports'
        row['hs_section_kind'] = kind
        row['hs_composite_rating'] = pe.get('rating', '') or ''
        row['hs_position'] = pe.get('position', '') or ''
        row['hs_stars'] = pe.get('stars', '') or ''
        year = pe.get('year')
        row['hs_class_year'] = str(year) if year else ''
        # JUCO suffix on ranks per user request (analysts must see at-a-glance
        # that ranks 1-100 are NOT comparable to 247Sports ranks 1-3000+)
        row['hs_national_rank'] = _apply_juco_suffix(pe.get('national_rank', '') or '', kind)
        row['hs_position_rank'] = _apply_juco_suffix(pe.get('position_rank', '') or '', kind)

    # ----- NFL Draft (always written if profile parsed it) -----
    draft = profile.get('draft_info') or {}
    row['draft_year']  = draft.get('draft_year', '') or ''
    row['draft_round'] = draft.get('draft_round', '') or ''
    row['draft_pick']  = draft.get('draft_pick', '') or ''
    row['draft_team']  = draft.get('draft_team', '') or ''

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
    """Fetch any missing profiles and apply them to rows. Cache key is
    (player_id, team) so the same player on different teams gets different
    profile views cached separately.
    """
    needed = {}     # cache_key -> (url, player_id)
    for r in rows:
        pid = r.get('247_id') or ''
        url = r.get('profile_url') or ''
        team = r.get('team', '')
        if not pid:
            r['profile_scraped'] = 'no_url'
            continue
        key = _cache_key(pid, team)
        if key in profile_cache:
            continue
        if not url:
            r['profile_scraped'] = 'no_url'
            continue
        full_url = _ensure_college_suffix(url, team)
        needed[key] = (full_url, pid)

    # Fetch missing profiles concurrently
    new_count = 0
    if needed:
        sem = asyncio.Semaphore(concurrency)
        tasks = [fetch_one_profile(context, sem, url, pid)
                 for url, pid in needed.values()]
        results = await asyncio.gather(*tasks)
        for key, res in zip(needed.keys(), results):
            profile_cache[key] = res
            if res.get('fetch_status') == 'ok':
                new_count += 1

    # Apply cache to rows
    for r in rows:
        pid = r.get('247_id') or ''
        team = r.get('team', '')
        if not pid:
            continue
        key = _cache_key(pid, team)
        if key in profile_cache:
            apply_profile_to_row(r, profile_cache[key], season)
    return new_count


# ---------- Roster page scrape ----------
async def scrape_team_season(page, team, season, verbose=False):
    """Scrape one (team, season) page."""
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
        return []

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
                # Validate checkpoint. Re-scrape if:
                #   - missing new schema columns
                #   - poisoned by legacy doubled-URL bug
                #   - >50% of profiled rows failed
                #   - height column is date-corrupted
                try:
                    head = pd.read_csv(ckpt, nrows=1)
                    has_new_schema = ('hs_class_year' in head.columns
                                       and 'draft_year' in head.columns
                                       and 'hs_composite_rating' in head.columns)
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
                    has_new_schema = False
                    poisoned = False
                if poisoned:
                    print(f"[{i+1}/{len(tasks)}] REDO {team} {season} "
                          f"(checkpoint poisoned)")
                elif has_new_schema or skip_profiles:
                    skipped += 1
                    if i % 20 == 0:
                        print(f"[{i+1}/{len(tasks)}] SKIP {team} {season} (cached)")
                    continue
                else:
                    print(f"[{i+1}/{len(tasks)}] BACKFILL {team} {season} "
                          f"(checkpoint exists but missing new schema)")

            if i > 0 and i % 30 == 0:
                await context.close()
                context = await browser.new_context(
                    user_agent=random.choice(USER_AGENTS),
                    viewport={'width': 1280, 'height': 900},
                )
                page = await context.new_page()

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
                print(f"\n⚠  ABORTING: {MAX_CONSECUTIVE_FAILURES} consecutive failures.")
                break

            await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        await browser.close()

    if not skip_profiles:
        flush_profile_cache(profile_cache)

    # Consolidate all checkpoints
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
        print(f"Distinct 247 IDs:    {full['247_id'].nunique():,}")
        print(f"Rows with 247 ID:    {full['247_id'].notna().sum():,}  "
              f"({full['247_id'].notna().mean():.1%})")
        if not skip_profiles:
            for f in (['hs_class_year', 'hs_composite_rating', 'hs_national_rank',
                       'hs_position_rank', 'hs_position', 'hs_stars']
                      + TRANSFER_FIELDS + DRAFT_FIELDS):
                filled = full[f].astype(str).str.strip().replace('nan', '').ne('').sum()
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
