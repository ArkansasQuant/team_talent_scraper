"""
Scrape active rosters for all FBS teams across 2018-2025 from 247sports.

Design principles (from scraping playbook):
  - Playwright + domcontentloaded (NOT networkidle)
  - Randomized delays + user-agent rotation
  - NEVER break-on-exception in loops — use continue + failure counter
  - Per-team-season checkpointing for crash recovery
  - Validate row counts against expected per-team norms
  - Post-run verification (row counts, ID format, null-ratio sanity)

Usage:
  python scrape_rosters.py --seasons 2018 2019 ... 2025 --output roster_full.csv
  python scrape_rosters.py --teams troy alabama --seasons 2022  (test mode)
"""
import argparse
import asyncio
import csv
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
MAX_CONSECUTIVE_FAILURES = 8     # abort if this many in a row fail
MAX_RETRIES_PER_PAGE = 3
CHECKPOINT_DIR = Path("checkpoints")

# Regex to extract 247 ID from player URL, e.g. /player/carlton-martial-91227/
PLAYER_URL_RE = re.compile(r'/player/([^/]*?)-(\d+)/?$')

# ---------- Extraction ----------
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

    # Strategy: find the main roster container. Player links list + data table
    # are typically siblings within a roster-specific block. We find the table
    # first, then find the player links that sit above it in the DOM.
    # Empirically: both render in source-order, so we zip them.

    # 1. Find all player links on the page
    player_anchors = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        m = PLAYER_URL_RE.search(href)
        if m:
            slug, pid = m.group(1), m.group(2)
            # The placeholder "empty" anchor is /player/--46120272/
            # The non-greedy regex captures slug='-' for --46120272 URLs.
            # Detect by checking for the known placeholder ID or an empty/dash-only slug.
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

    # Filter to roster-context anchors. We need to disambiguate roster links
    # from elsewhere on the page (nav, ads, etc.). Strategy: find the anchors
    # whose parent is inside the main roster section. The roster table uses a
    # container the scraper needs to locate.
    #
    # Fallback: if we can't locate a clean container, take the first N anchors
    # where N matches the data table length.

    # 2. Find roster data rows (the <tr> elements with the columns we need)
    data_rows = []
    for table in soup.find_all('table'):
        trs = table.find_all('tr')
        if len(trs) < 2: continue
        # Check if this table looks like a roster (has Jersey/POS/Height etc. in header)
        header_cells = [c.get_text(strip=True).lower() for c in trs[0].find_all(['th','td'])]
        header_text = ' '.join(header_cells)
        if not ('jersey' in header_text or 'pos' in header_text or 'height' in header_text):
            continue
        # This is the roster data table
        for tr in trs[1:]:
            tds = [td.get_text(strip=True) for td in tr.find_all(['td','th'])]
            if len(tds) < 6:
                continue
            data_rows.append(tds)
        break  # first matching table

    if not data_rows:
        return []   # no roster data found — will be logged upstream

    # 3. The player anchors include navigation, ads, etc. We want the anchors
    # that correspond 1:1 with the data rows. Heuristic: the roster-local
    # anchors appear as a contiguous block of len(data_rows) player links.
    # Find the longest contiguous window of player anchors equal to the
    # number of data rows.
    n = len(data_rows)
    best_window = None
    for i in range(len(player_anchors) - n + 1):
        window = player_anchors[i:i+n]
        # Score: count of non-null 247_ids (real players) — maximize.
        # Assumes the roster block has the most real profile links in one run.
        score = sum(1 for a in window if a['247_id'])
        if best_window is None or score > best_window['score']:
            best_window = {'anchors': window, 'score': score, 'start': i}

    if not best_window or best_window['score'] == 0:
        # No plausible window — use first n anchors as a last resort,
        # but this will likely indicate a page structure change we should flag.
        anchors = player_anchors[:n] if len(player_anchors) >= n else None
        if anchors is None:
            return []
    else:
        anchors = best_window['anchors']

    # 4. Zip anchors with data rows
    for anchor, data in zip(anchors, data_rows):
        # Data table cols: Jersey | POS | Height | Weight | Yr | Age | High School | Rating
        # Pad to 8 cols if short (some rows have fewer cells rendered)
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
        rows.append(row)
    return rows

# ---------- Scrape one team-season ----------
async def scrape_team_season(page, team, season, verbose=False):
    """Scrape one (team, season) page. Returns list of row dicts on success,
       or None on non-recoverable failure. Caller handles retries."""
    url = team_url(team, season)
    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=NAV_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        if verbose: print(f"  TIMEOUT goto {team} {season}")
        return None
    except Exception as e:
        if verbose: print(f"  ERROR goto {team} {season}: {type(e).__name__}: {e}")
        return None

    # Give the roster table a moment to render client-side (it's static HTML
    # but some content is deferred). Don't use networkidle.
    try:
        await page.wait_for_selector('table', timeout=10000)
    except PlaywrightTimeoutError:
        if verbose: print(f"  NO TABLE for {team} {season}")
        return []   # valid empty — team-season has no roster (probably pre-FBS)

    html = await page.content()
    rows = parse_roster_html(html, team, season)
    return rows

# ---------- Orchestrator ----------
async def run(seasons, teams, output, skip_existing=True):
    CHECKPOINT_DIR.mkdir(exist_ok=True)
    for s in seasons:
        (CHECKPOINT_DIR / str(s)).mkdir(parents=True, exist_ok=True)

    tasks = []
    for season in seasons:
        for team in teams:
            if not is_fbs_in_year(team, season):
                continue
            tasks.append((team, season))

    print(f"Tasks to run: {len(tasks)}")
    completed = 0
    skipped = 0
    consecutive_failures = 0
    started_at = time.time()

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
                skipped += 1
                if i % 20 == 0:
                    print(f"[{i+1}/{len(tasks)}] SKIP {team} {season} (cached)")
                continue

            # Rotate user-agent every ~30 requests (new context)
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
                await asyncio.sleep(5 + attempt * 3)   # backoff between retries

            status = ''
            if rows is None:
                consecutive_failures += 1
                status = 'FAIL'
                # Write empty checkpoint with a marker so we don't re-try it
                # indefinitely; user can delete it manually to force retry.
                with open(ckpt, 'w', newline='') as f:
                    f.write('# SCRAPE FAILED — delete this file to retry\n')
            else:
                consecutive_failures = 0
                if rows:
                    pd.DataFrame(rows).to_csv(ckpt, index=False)
                    status = f'OK ({len(rows)} rows)'
                else:
                    # Zero rows but no error — likely pre-FBS season or page-structure change
                    pd.DataFrame(columns=['247_id','player_name','team','season','jersey',
                                          'position','height','weight','class_yr','age',
                                          'high_school','rating_247','profile_url','scrape_ts']
                                ).to_csv(ckpt, index=False)
                    status = 'OK (0 rows)'

            completed += 1
            elapsed = time.time() - started_at
            rate = completed / elapsed if elapsed > 0 else 0
            eta_s = (len(tasks) - skipped - completed) / rate if rate > 0 else 0
            print(f"[{i+1}/{len(tasks)}] {team:24s} {season}  {status}   "
                  f"elapsed={elapsed/60:.1f}m  eta={eta_s/60:.0f}m")

            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                print(f"\n⚠  ABORTING: {MAX_CONSECUTIVE_FAILURES} consecutive failures. "
                      f"Likely rate-limited or structural change. Inspect & rerun.")
                break

            # Randomized delay
            await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        await browser.close()

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
        full.to_csv(output, index=False)
        print(f"Wrote {len(full):,} total rows to {output}")
        # Post-run validation
        print(f"\n=== Post-run validation ===")
        print(f"Distinct 247 IDs: {full['247_id'].nunique():,}")
        print(f"Rows with 247 ID:    {full['247_id'].notna().sum():,}  "
              f"({full['247_id'].notna().mean():.1%})")
        print(f"Rows without (unrated): {full['247_id'].isna().sum():,}")
        print(f"\nRows per season:")
        print(full.groupby('season').size().to_string())
        print(f"\nTeams with <40 or >200 roster rows (suspicious):")
        team_season_counts = full.groupby(['team','season']).size()
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
    ap.add_argument('--output', default='roster_full_2018_2025.csv')
    ap.add_argument('--force', action='store_true', help='Ignore checkpoints, re-scrape all')
    args = ap.parse_args()

    teams = args.teams if args.teams else all_teams()
    # Validate team names
    bad = [t for t in teams if t not in TEAM_URLS]
    if bad:
        print(f"Unknown teams: {bad}"); sys.exit(1)

    asyncio.run(run(args.seasons, teams, args.output, skip_existing=not args.force))

if __name__ == '__main__':
    main()
