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

Design principles:
  - Profile fetch ported from working transfer-portal scraper:
      wait_until="commit", wait_for_selector(".name, h1.name", 15s)
  - Profile navigation ported from working HS recruiting scraper:
      navigate_to_recruiting_profile() → clicks "View recruiting profile"
      navigate_to_hs_profile() → clicks "(HS)" link on JUCO/NCAA profiles
  - Section parser split by section kind based on observed DOM:
      Transfer section <li>s use positionKey= in href (e.g. TransferPortalTop/?positionKey=25)
      Prospect section <li>s use Position= in href (e.g. recruitrankings/?Position=CB)
      State rows have State= in href (skip)
      National rank rows have InstitutionGroup=HighSchool with no Position= or State=
  - Concurrency dropped to 3 (was 6) to avoid 247 rate limiting
  - GLOBAL profile cache keyed by 247_id only
  - Cache flushed after each team-season for mid-run resilience
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
PROFILE_NAV_TIMEOUT_MS = 60000        # match transfer scraper
PROFILE_SELECTOR_TIMEOUT_MS = 15000   # match transfer scraper (was 5000)
PROFILE_POST_NAV_WAIT_MS = 1000
MAX_CONSECUTIVE_FAILURES = 8
MAX_RETRIES_PER_PAGE = 3
MAX_PROFILE_RETRIES = 2
DEFAULT_PROFILE_CONCURRENCY = 3       # was 6
PROFILE_DELAY_MIN = 0.3
PROFILE_DELAY_MAX = 1.0

# Schema 10 changes:
#   - _parse_one_section split into _parse_transfer_section and _parse_prospect_section
#     based on observed DOM: transfer uses positionKey= in href, prospect uses Position=.
#   - National rank in prospect sections detected via href containing
#     InstitutionGroup=HighSchool *without* Position= or State= (handles
#     players without a NATL label row).
#   - Position label preserved even when its rank value is N/A.
PROFILE_CACHE_SCHEMA = 10

CHECKPOINT_DIR = Path("checkpoints")
PROFILE_CACHE_FILE = CHECKPOINT_DIR / "profiles_cache.json"

PLAYER_URL_RE = re.compile(r'/player/([^/]*?)-(\d+)/?$')

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
    """True if a parsed value should be treated as missing for numeric fields."""
    if s is None:
        return True
    s = str(s).strip().upper()
    return s in ('', 'N/A', 'NA', '—', '–', '-')


def _parse_rank(text):
    """Extract a numeric rank from text like '#123' or '123' or '1,234'."""
    if not text:
        return ''
    m = re.search(r'#?\s*([\d,]+)', text)
    if m:
        return m.group(1).replace(',', '')
    return ''


# ---------- Roster page extraction ----------
def parse_roster_html(html, team, season):
    """Extract roster rows from a team roster page. Captures the decimal
    composite rating from the roster table directly into hs_composite_rating.
    """
    soup = BeautifulSoup(html, 'lxml')
    rows = []

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
                if href.startswith('http://') or href.startswith('https://'):
                    full_url = href.rstrip('/') + '/'
                else:
                    full_url = f"https://247sports.com{href.rstrip('/')}/"
                player_anchors.append({
                    'name': name,
                    '247_id': None if is_placeholder else pid,
                    'profile_url': None if is_placeholder else full_url,
                })

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

    for anchor, data in zip(anchors, data_rows):
        data = (data + [''] * 8)[:8]
        jersey, pos, height, weight, yr, age, hs, rating = data
        m_h = re.match(r'^\s*(\d{1,2})-(\d{1,2})\s*$', height or '')
        if m_h:
            height = f"{m_h.group(1)}'{m_h.group(2)}\""
        composite_from_roster = ''
        if rating:
            m_rating = re.match(r'^\s*(\d+\.\d+)', rating)
            if m_rating:
                composite_from_roster = m_rating.group(1)
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
            'profile_url': anchor['profile_url'] or '',
            'scrape_ts': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
        }
        for f in HS_FIELDS + TRANSFER_FIELDS + STATUS_FIELDS:
            row[f] = ''
        row['hs_composite_rating'] = composite_from_roster
        rows.append(row)
    return rows


# ---------- Profile navigation helpers (ported from HS recruiting scraper) ----------
async def _navigate_to_recruiting_profile(page):
    """Click 'View recruiting profile' if present — required when 247 lands
    us on a player's college/NFL view instead of their recruiting view."""
    try:
        recruiting_link = page.locator(
            'a:has-text("View recruiting profile"), a:has-text("Recruiting Profile")'
        )
        if await recruiting_link.count() > 0:
            await recruiting_link.first.click()
            await page.wait_for_load_state('domcontentloaded', timeout=30000)
            await page.wait_for_timeout(PROFILE_POST_NAV_WAIT_MS)
            return True
    except Exception:
        pass
    return False


async def _navigate_to_hs_profile(page):
    """If on a JUCO/NCAA profile, navigate to the (HS) profile."""
    try:
        hs_href = await page.evaluate("""
            () => {
                const links = [...document.querySelectorAll('a')];
                const hs = links.find(a => a.textContent.includes('(HS)'));
                return hs ? hs.href : null;
            }
        """)
        if hs_href:
            await page.goto(hs_href, wait_until='domcontentloaded', timeout=30000)
            await page.wait_for_timeout(PROFILE_POST_NAV_WAIT_MS)
            return True
    except Exception:
        pass
    return False


# ---------- Section parsing ----------
def _parse_section_common(section, is_juco):
    """Parse rating, year, and stars from a rankings section.

    Selectors (verified against live DOM screenshots):
      Rating container: .rank-block (may contain a .rank-year span for year)
      Stars: span.icon-starsolid.yellow (lightgrey = unfilled, skipped)
    """
    out = {
        'rating': '', 'year': None, 'stars': '',
    }

    rating_block = (section.select_one('.rank-block')
                    or section.select_one('.score')
                    or section.select_one('.rating'))
    if rating_block:
        # Extract year first from .rank-year span if present
        year_tag = rating_block.select_one('.rank-year')
        if year_tag:
            m_year = re.search(r'\((\d{4})\)', year_tag.get_text(' ', strip=True))
            if m_year:
                out['year'] = int(m_year.group(1))
        # Get full text and parse leading number for the rating
        rating_text = rating_block.get_text(' ', strip=True)
        if not _is_na(rating_text):
            m_rating = re.match(r'^\s*(\d+(?:\.\d+)?)', rating_text)
            if m_rating:
                out['rating'] = m_rating.group(1)
        # Fallback for year embedded in main text
        if out['year'] is None:
            m_year2 = re.search(r'\((\d{4})\)', rating_text)
            if m_year2:
                out['year'] = int(m_year2.group(1))

    if is_juco:
        out['stars'] = 'JUCO'
    else:
        stars = section.select(
            'span.icon-starsolid.yellow, i.icon-starsolid.yellow'
        )
        if stars:
            out['stars'] = str(min(len(stars), 5))
    return out


def _parse_transfer_section(section):
    """Parse a transfer rankings section. DOM shape (verified live 2026-05-19):

      <section class="rankings-section">
        <h3 class="title">247Sports Transfer Rankings</h3>
        <div class="ranking">
          <div class="stars-block">…</div>
          <div class="rank-block">85 <span class="rank-year">(2021)</span></div>
        </div>
        <ul class="ranks-list">
          <li><b>OVR</b><a href=".../TransferPortalTop/"><strong>88</strong></a></li>
          <li><b>S</b><a href=".../TransferPortalTop/?positionKey=25"><strong>13</strong></a></li>
        </ul>
      </section>

    Identification rules for transfer <li>:
      - Label 'OVR' → overall_rank
      - href contains 'positionKey=' → position_rank (label is position code)
    """
    out = {'kind': 'Transfer'}
    out.update(_parse_section_common(section, is_juco=False))
    out.update({
        'overall_rank': '', 'national_rank': '',
        'position_rank': '', 'position': '',
    })

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
            # Defensive fallback for non-OVR rows without positionKey=
            out['position'] = label
            if not value_is_na:
                out['position_rank'] = _parse_rank(value)
    return out


def _parse_prospect_section(section, is_juco):
    """Parse a 247Sports / Composite / JUCO prospect section. DOM shape
    (verified live 2026-05-19):

      <section class="rankings-section">
        <h3 class="title">247Sports</h3>
        <div class="ranking">
          <div class="stars-block">…</div>
          <div class="rank-block">80</div>
        </div>
        <ul class="ranks-list">
          <li><b>CB</b><a href=".../?InstitutionGroup=HighSchool&Position=CB"><strong>192</strong></a></li>
          <li><b>TX</b><a href=".../?InstitutionGroup=HighSchool&State=TX"><strong>263</strong></a></li>
        </ul>
      </section>

    Some players additionally have a national rank row (no Position=, no State=).

    Identification rules for prospect <li>:
      - href contains 'State=' → state rank (skip)
      - href contains 'Position=' → position_rank (label is position code)
      - href contains 'InstitutionGroup=HighSchool' without Position= or
        State= → national_rank
      - Label 'NATL' / 'NATIONAL' (older markup fallback) → national_rank
      - Label 'OVR' (rare for prospect) → overall_rank
    """
    out = {'kind': 'JUCO' if is_juco else '247Sports'}  # caller may overwrite
    out.update(_parse_section_common(section, is_juco=is_juco))
    out.update({
        'overall_rank': '', 'national_rank': '',
        'position_rank': '', 'position': '',
    })

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

        # State row — always skip
        if 'State=' in href or 'state=' in href:
            continue

        # Position rank: href has Position= (CB, WR, etc.)
        if 'Position=' in href:
            if not out['position']:
                out['position'] = label
                if not value_is_na:
                    out['position_rank'] = _parse_rank(value)
            continue

        # National rank: href has InstitutionGroup=HighSchool with no Position=
        # (or no State= since we already skipped those above)
        if 'InstitutionGroup=HighSchool' in href:
            if not value_is_na:
                out['national_rank'] = _parse_rank(value)
            continue

        # Label fallbacks for older markups / variants
        if 'OVR' in label or 'OVERALL' in label:
            if not value_is_na:
                out['overall_rank'] = _parse_rank(value)
        elif 'NATL' in label or 'NATIONAL' in label:
            if not value_is_na:
                out['national_rank'] = _parse_rank(value)
        elif not out['position']:
            # Last-resort position label capture (preserves label even if value is N/A)
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
    """Pull origin/destination teams, ALL transfer events, and a prospect event.

    Section title variants:
      "247Sports Transfer Rankings"  → transfer
      "247Sports"                    → HS recruiting (whole-number scout rating)
      "247Sports Composite®"         → HS composite (decimal rating)
      "JUCO" anywhere in title       → JUCO recruiting

    Section selector broadened to match HS recruiting scraper:
      'section.rankings, section.rankings-section, div.ranking-section'
    """
    soup = BeautifulSoup(html, 'lxml')
    result = {
        'origin_team': '',
        'destination_team': '',
        'transfer_events': [],
        'prospect_event': None,
        'section_titles': [],
    }

    team_header = soup.select_one('.team-info-section header h2')
    if team_header:
        result['origin_team'] = team_header.get_text(strip=True)

    commit_banner = soup.select_one('.commit-banner span')
    if commit_banner:
        txt = commit_banner.get_text(strip=True)
        if txt and txt.lower() != 'commit':
            result['destination_team'] = txt

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
    """Choose the transfer event for this roster season."""
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
    """Append /college-{team_id}/ if missing."""
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

    Fetch pattern ported from working transfer-portal scraper:
      - wait_until="commit"  (let page render naturally)
      - wait_for_selector(".name, h1.name", timeout=15000)

    Navigation flow:
      1. goto(url) — initial landing
      2. navigate_to_recruiting_profile() — click "View recruiting profile" if shown
      3. navigate_to_hs_profile() — click "(HS)" if on JUCO/NCAA variant
      4. wait_for(.name) — confirm content rendered
      5. content() + parse
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

                await _navigate_to_recruiting_profile(page)
                await _navigate_to_hs_profile(page)

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
    """' (JUCO)' suffix only on real (non-NA, non-empty) values."""
    if not value or _is_na(value):
        return value
    if kind == 'JUCO' and '(JUCO)' not in str(value):
        return f"{value} (JUCO)"
    return value


def apply_profile_to_row(row, profile, season):
    """Write HS and transfer fields from profile into the row.

    hs_composite_rating is pre-populated from the roster table — we do NOT
    overwrite it here. The profile-parsed value goes to hs_scout_rating.
    """
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

    row['transfer_origin_team'] = profile.get('origin_team', '') or ''
    row['transfer_destination_team'] = profile.get('destination_team', '') or ''

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
    """Fetch missing profiles and apply them to rows."""
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
                try:
                    head = pd.read_csv(ckpt, nrows=1)
                    has_new_schema = (
                        'hs_class_year' in head.columns
                        and 'hs_composite_rating' in head.columns
                        and 'hs_scout_rating' in head.columns
                        and 'draft_year' not in head.columns
                    )
                    sample = pd.read_csv(
                        ckpt,
                        usecols=lambda c: c in ('profile_url', 'profile_scraped',
                                                'height', 'hs_composite_rating',
                                                'hs_section_kind', 'hs_scout_rating',
                                                'transfer_origin_team'),
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
                    # Detect last-run skeleton poisoning: kind set but neither
                    # scout rating nor origin team populated for most rows.
                    skeleton_poisoned = False
                    if ('hs_section_kind' in sample.columns
                            and 'hs_scout_rating' in sample.columns
                            and 'transfer_origin_team' in sample.columns):
                        kind_set = sample['hs_section_kind'].str.strip().ne('')
                        scout_empty = sample['hs_scout_rating'].str.strip().eq('')
                        origin_empty = sample['transfer_origin_team'].str.strip().eq('')
                        if kind_set.sum() > 5:
                            skeleton_share = (kind_set & scout_empty & origin_empty).sum() / max(kind_set.sum(), 1)
                            skeleton_poisoned = skeleton_share > 0.7
                    poisoned = (bad_url_share > 0.1
                                or fail_share > 0.5
                                or height_corrupt
                                or skeleton_poisoned)
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
                          f"(checkpoint exists but schema mismatch)")

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

        # Post-run validation
        print(f"\n=== Post-run validation ===")
        print(f"Distinct 247 IDs:    {full['247_id'].nunique():,}")
        print(f"Rows with 247 ID:    {full['247_id'].notna().sum():,}  "
              f"({full['247_id'].notna().mean():.1%})")
        if not skip_profiles:
            for f in (HS_FIELDS + TRANSFER_FIELDS):
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
