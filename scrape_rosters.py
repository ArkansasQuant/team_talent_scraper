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

DELAY_MIN = 3.0
DELAY_MAX = 8.0
NAV_TIMEOUT_MS = 30000
PROFILE_NAV_TIMEOUT_MS = 45000
MAX_CONSECUTIVE_FAILURES = 8     # abort if this many in a row fail
MAX_RETRIES_PER_PAGE = 3
MAX_PROFILE_RETRIES = 3
DEFAULT_PROFILE_CONCURRENCY = 4
PROFILE_DELAY_MIN = 0.8
PROFILE_DELAY_MAX = 2.2

CHECKPOINT_DIR = Path("checkpoints")
PROFILE_CACHE_FILE = CHECKPOINT_DIR / "profiles_cache.csv"

# Regex to extract 247 ID from player URL, e.g. /player/carlton-martial-91227/
PLAYER_URL_RE = re.compile(r'/player/([^/]*?)-(\d+)/?$')

# Columns added by the profile enrichment step
TRANSFER_FIELDS = [
    'transfer_origin_team',       # team they came from (pre-roster)
    'transfer_destination_team',  # team they committed to (per profile commit banner)
    'transfer_rating',            # numeric 247 transfer rating, e.g. "88"
    'transfer_overall_rank',      # transfer portal overall rank
    'transfer_position_rank',     # transfer portal position rank
    'transfer_position',          # position label associated with rank (QB, WR, etc.)
    'transfer_class_year',        # transfer class year, e.g. "2025"
    'transfer_stars',             # 1-5 star count from transfer section
    'profile_scraped',            # 'ok' | 'failed' | 'skipped' | 'no_url'
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
                player_anchors.append({
                    'name': name,
                    '247_id': None if is_placeholder else pid,
                    'profile_url': None if is_placeholder else f"https://247sports.com{href.rstrip('/')}/",
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
def parse_player_profile(html):
    """
    Pull transfer subsection fields from a player profile page.

    Mirrors the logic from the transfer-portal scraper:
      - .team-info-section header h2  → origin team (the team they came from)
      - .commit-banner span           → destination team (where they landed)
      - section.rankings-section with title containing "Transfer":
            .rank-block                 → rating + class year (YYYY)
            li > b "OVR"                → overall rank
            li > b position label       → position rank
            span.icon-starsolid.yellow  → star count
    """
    soup = BeautifulSoup(html, 'lxml')
    out = {f: '' for f in TRANSFER_FIELDS}

    # Origin (the team listed on the player's profile = where they came from)
    team_header = soup.select_one('.team-info-section header h2')
    if team_header:
        out['transfer_origin_team'] = team_header.get_text(strip=True)

    # Destination (commit banner = team they committed to / transferred to)
    commit_banner = soup.select_one('.commit-banner span')
    if commit_banner:
        txt = commit_banner.get_text(strip=True)
        if txt and txt.lower() != 'commit':
            out['transfer_destination_team'] = txt

    # Walk all rankings sections, find the Transfer one
    for section in soup.select('section.rankings-section'):
        title_tag = section.select_one('h3.title')
        if not title_tag:
            continue
        title = title_tag.get_text(strip=True)
        if 'Transfer' not in title:
            continue

        # Stars
        stars = section.select('span.icon-starsolid.yellow')
        if stars:
            out['transfer_stars'] = str(min(len(stars), 5))

        # Rating + class year — both live inside .rank-block
        rating_block = section.select_one('.rank-block')
        if rating_block:
            rating_text = rating_block.get_text(strip=True)
            m_rating = re.search(r'(\d{2,3}(?:\.\d+)?)', rating_text)
            if m_rating:
                out['transfer_rating'] = m_rating.group(1)
            m_year = re.search(r'\((\d{4})\)', rating_text)
            if m_year:
                out['transfer_class_year'] = m_year.group(1)

        # Ranks (OVR + position)
        for li in section.select('li'):
            bold = li.find('b')
            strong = li.find('strong')
            if not bold or not strong:
                continue
            label = bold.get_text(strip=True).upper()
            value = strong.get_text(strip=True)
            if 'OVR' in label:
                out['transfer_overall_rank'] = value
            elif not out['transfer_position_rank']:
                # First non-OVR rank in the transfer section is the position rank
                out['transfer_position_rank'] = value
                out['transfer_position'] = label
        break  # only one Transfer section per profile

    return out


# ---------- Profile cache ----------
def load_profile_cache():
    """Load cached profile lookups from disk into a dict keyed by 247_id."""
    if not PROFILE_CACHE_FILE.exists():
        return {}
    try:
        df = pd.read_csv(PROFILE_CACHE_FILE, dtype=str).fillna('')
        cache = {}
        for _, row in df.iterrows():
            pid = row.get('247_id', '')
            if pid:
                cache[pid] = {f: row.get(f, '') for f in TRANSFER_FIELDS}
        print(f"  Loaded {len(cache):,} cached profiles from {PROFILE_CACHE_FILE}")
        return cache
    except Exception as e:
        print(f"  WARN: could not read profile cache ({e}); starting empty")
        return {}


def flush_profile_cache(cache):
    """Persist the profile cache as a single CSV (safe atomic write)."""
    if not cache:
        return
    PROFILE_CACHE_FILE.parent.mkdir(exist_ok=True)
    rows = []
    for pid, fields in cache.items():
        row = {'247_id': pid}
        row.update({f: fields.get(f, '') for f in TRANSFER_FIELDS})
        rows.append(row)
    tmp = PROFILE_CACHE_FILE.with_suffix('.tmp')
    pd.DataFrame(rows).to_csv(tmp, index=False)
    tmp.replace(PROFILE_CACHE_FILE)


# ---------- Profile fetcher ----------
async def fetch_one_profile(context, sem, url, player_id):
    """Fetch & parse a single player profile. Returns a TRANSFER_FIELDS dict
    on success, or None on terminal failure."""
    async with sem:
        for attempt in range(MAX_PROFILE_RETRIES):
            page = await context.new_page()
            # Speed: block heavy assets — we only need the HTML.
            await page.route(
                "**/*.{png,jpg,jpeg,gif,svg,webp,mp4,webm,woff,woff2,ttf,otf,css}",
                lambda route: route.abort(),
            )
            try:
                await asyncio.sleep(random.uniform(PROFILE_DELAY_MIN, PROFILE_DELAY_MAX))
                await page.goto(url, timeout=PROFILE_NAV_TIMEOUT_MS, wait_until='domcontentloaded')
                # Give the rankings section a moment; tolerate absence
                try:
                    await page.wait_for_selector('section.rankings-section, .name, h1.name',
                                                 timeout=8000)
                except PlaywrightTimeoutError:
                    pass
                html = await page.content()
                await page.close()
                if len(html) < 2000:
                    raise RuntimeError("suspiciously small profile HTML")
                fields = parse_player_profile(html)
                fields['profile_scraped'] = 'ok'
                return fields
            except Exception as e:
                try:
                    await page.close()
                except Exception:
                    pass
                if attempt < MAX_PROFILE_RETRIES - 1:
                    await asyncio.sleep(2 + attempt * 2)
                else:
                    print(f"    profile FAIL ({player_id}): {type(e).__name__}: {e}")
                    return None
        return None


async def enrich_with_profiles(rows, context, profile_cache, concurrency):
    """For every row missing transfer fields, fetch the profile (or use cache).
    Mutates rows in-place. Returns the count of newly-cached profiles."""
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
        needed[pid] = url

    # 2. Fetch missing profiles concurrently
    new_count = 0
    if needed:
        sem = asyncio.Semaphore(concurrency)
        tasks = [fetch_one_profile(context, sem, url, pid)
                 for pid, url in needed.items()]
        results = await asyncio.gather(*tasks)
        for (pid, _url), res in zip(needed.items(), results):
            if res is None:
                profile_cache[pid] = {f: '' for f in TRANSFER_FIELDS}
                profile_cache[pid]['profile_scraped'] = 'failed'
            else:
                profile_cache[pid] = res
                new_count += 1

    # 3. Apply cache to rows
    for r in rows:
        pid = r.get('247_id') or ''
        if pid and pid in profile_cache:
            for f in TRANSFER_FIELDS:
                r[f] = profile_cache[pid].get(f, '')
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
                # Double-check the existing checkpoint already has transfer cols
                # (so a re-run after upgrading still backfills if needed).
                try:
                    head = pd.read_csv(ckpt, nrows=1)
                    has_transfer_cols = 'transfer_origin_team' in head.columns
                except Exception:
                    has_transfer_cols = False
                if has_transfer_cols or skip_profiles:
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
                                rows, context, profile_cache, profile_concurrency,
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
