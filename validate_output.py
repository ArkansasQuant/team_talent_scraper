"""Post-run validator for roster CSVs.

Run after a scrape completes:
    python tests/validate_output.py path/to/roster_2024.csv

Surfaces:
  - profile_scraped status distribution (sanity check)
  - % of rows with each enrichment field populated
  - 5 example rows per status bucket
  - Common bugs to look for (date-corrupted heights, missing HS data, etc.)

Exit code 0 if output looks healthy, 1 if any red flags found.
"""
import sys
from pathlib import Path

import pandas as pd

# --- Health thresholds --------------------------------------------------
# These are conservative. A typical Power-5 roster scrape with profile
# enrichment enabled should easily clear them.
MIN_PROFILE_OK_SHARE        = 0.70   # >=70% of profiled rows should be ok or ok_no_transfer
MAX_FAILED_SHARE            = 0.05   # <5% of rows should be 'failed'
MIN_HS_POPULATED_SHARE      = 0.50   # >=50% of all rows should have hs_class_year
MIN_TRANSFER_AMONG_OK_SHARE = 0.10   # >=10% of 'ok' rows should have a transfer rating

RED_FLAGS = []


def _flag(msg):
    RED_FLAGS.append(msg)
    print(f"  [WARN] {msg}")


def main(csv_path):
    path = Path(csv_path)
    if not path.exists():
        print(f"ERROR: file not found: {path}")
        sys.exit(2)

    df = pd.read_csv(path, dtype=str).fillna('')
    n = len(df)
    print(f"\nLoaded {n:,} rows from {path.name}\n")

    # --- 1. profile_scraped status distribution ---
    print("=" * 60)
    print("profile_scraped status distribution")
    print("=" * 60)
    if 'profile_scraped' not in df.columns:
        _flag("profile_scraped column MISSING \u2014 schema mismatch")
        sys.exit(1)
    counts = df['profile_scraped'].value_counts(dropna=False)
    for status, count in counts.items():
        pct = count / n * 100
        print(f"  {status:20s} {count:>8,}  ({pct:5.1f}%)")

    profiled = counts.get('ok', 0) + counts.get('ok_no_transfer', 0)
    failed = counts.get('failed', 0)
    no_url = counts.get('no_url', 0)

    if (n - no_url) > 0:
        ok_share = profiled / (n - no_url)
        if ok_share < MIN_PROFILE_OK_SHARE:
            _flag(f"Only {ok_share:.0%} of profile-eligible rows succeeded "
                  f"(threshold {MIN_PROFILE_OK_SHARE:.0%}) \u2014 fetch problems likely")

    fail_share = failed / n
    if fail_share > MAX_FAILED_SHARE:
        _flag(f"{fail_share:.0%} of rows are 'failed' "
              f"(threshold {MAX_FAILED_SHARE:.0%}) \u2014 247 may be rate-limiting")

    # --- 2. Field-population breakdown ---
    print("\n" + "=" * 60)
    print("Field population (% non-empty)")
    print("=" * 60)
    transfer_cols = [c for c in df.columns if c.startswith('transfer_')]
    hs_cols       = [c for c in df.columns if c.startswith('hs_')]
    for col in sorted(transfer_cols + hs_cols):
        non_empty = (df[col].astype(str).str.strip() != '').sum()
        pct = non_empty / n * 100
        print(f"  {col:32s} {non_empty:>8,}  ({pct:5.1f}%)")

    # HS data should populate broadly (most college players have an HS profile)
    if 'hs_class_year' in df.columns:
        hs_share = (df['hs_class_year'].astype(str).str.strip() != '').sum() / n
        if hs_share < MIN_HS_POPULATED_SHARE:
            _flag(f"hs_class_year populated on only {hs_share:.0%} of rows "
                  f"(threshold {MIN_HS_POPULATED_SHARE:.0%}) \u2014 prospect parsing may be broken")

    # Among 'ok' rows, transfer fields should be populated
    if 'profile_scraped' in df.columns and 'transfer_rating' in df.columns:
        ok_rows = df[df['profile_scraped'] == 'ok']
        if len(ok_rows) > 0:
            tr_share = (ok_rows['transfer_rating'].astype(str).str.strip() != '').sum() / len(ok_rows)
            print(f"\n  Among status='ok' rows: {tr_share:.0%} have transfer_rating populated")
            if tr_share < MIN_TRANSFER_AMONG_OK_SHARE:
                _flag(f"Only {tr_share:.0%} of 'ok' rows have transfer_rating "
                      f"(threshold {MIN_TRANSFER_AMONG_OK_SHARE:.0%})")

    # --- 3. Known-bug checks ---
    print("\n" + "=" * 60)
    print("Known-bug checks")
    print("=" * 60)

    # Date-corrupted heights (5-10 -> 10-May style)
    if 'height' in df.columns:
        bad_heights = df['height'].str.contains(r'May|Jun|Jul|Aug', regex=True, na=False).sum()
        if bad_heights:
            _flag(f"{bad_heights} rows have date-corrupted heights (e.g. '10-May')")
        else:
            print("  [OK ] No date-corrupted heights found")

    # Doubled URLs (legacy bug)
    if 'profile_url' in df.columns:
        bad_urls = df['profile_url'].str.contains('comhttps', na=False).sum()
        if bad_urls:
            _flag(f"{bad_urls} rows have malformed 'comhttps' URLs (legacy bug returned)")
        else:
            print("  [OK ] No malformed profile URLs")

    # Origin == destination (the .team-block bug)
    if 'transfer_origin_team' in df.columns and 'transfer_destination_team' in df.columns:
        same = ((df['transfer_origin_team'] != '') &
                (df['transfer_origin_team'] == df['transfer_destination_team'])).sum()
        if same > 0:
            _flag(f"{same} rows have origin==destination (selector bug regression?)")
        else:
            print("  [OK ] No origin==destination rows")

    # Transfer year in the future relative to season (Bug 4 regression)
    if {'transfer_class_year', 'season'}.issubset(df.columns):
        try:
            ty = pd.to_numeric(df['transfer_class_year'], errors='coerce')
            sn = pd.to_numeric(df['season'], errors='coerce')
            future = ((ty > sn) & ty.notna() & sn.notna()).sum()
            if future > 0:
                _flag(f"{future} rows have transfer_class_year > season (future pollution)")
            else:
                print("  [OK ] No future-transfer pollution")
        except Exception:
            pass

    # --- 4. Sample rows per status ---
    print("\n" + "=" * 60)
    print("Sample rows (3 per status bucket)")
    print("=" * 60)
    sample_cols = [c for c in
                   ['team', 'season', 'player_name', 'position', 'class_yr',
                    'profile_scraped', 'transfer_origin_team',
                    'transfer_class_year', 'transfer_rating',
                    'hs_class_year', 'hs_rating', 'hs_national_rank']
                   if c in df.columns]
    for status in df['profile_scraped'].dropna().unique():
        subset = df[df['profile_scraped'] == status][sample_cols].head(3)
        print(f"\n  --- profile_scraped = '{status}' ---")
        print(subset.to_string(index=False))

    # --- Verdict ---
    print("\n" + "=" * 60)
    if RED_FLAGS:
        print(f"FAIL - {len(RED_FLAGS)} red flag(s) found:")
        for f in RED_FLAGS:
            print(f"  - {f}")
        sys.exit(1)
    print("OK - output looks healthy")
    sys.exit(0)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python tests/validate_output.py path/to/roster_YYYY.csv")
        sys.exit(2)
    main(sys.argv[1])
