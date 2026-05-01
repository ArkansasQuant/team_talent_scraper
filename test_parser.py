"""Sanity tests for the profile parser + enrichment helpers.

Run from repo root:
    python tests/test_parser.py

These tests are HTML-based fixtures \u2014 no network, no Playwright, no pandas.
They cover the four bugs that were silently corrupting roster output:
  Bug 1: parse_player_profile dropped HS recruiting / JUCO data
  Bug 2: _parse_one_section unconditionally skipped NATL field
  Bug 3: TRANSFER_FIELDS schema didn't include hs_* columns
  Bug 4: pick_transfer_event silently dropped events with year=None

Plus regression coverage for the original good behavior:
  - Iterates ALL Transfer sections (no break after first)
  - Season-aware event picking (no future pollution)
  - Rating regex anchored to start of rank-block
  - State rank vs position rank disambiguation via href
"""
import os
import sys

# Locate the scraper module relative to this test file (one level up)
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

# Stub pandas + playwright \u2014 tests don't need them, and CI may not have them
import types
if 'pandas' not in sys.modules:
    try:
        import pandas  # noqa: F401
    except ImportError:
        sys.modules['pandas'] = types.ModuleType('pandas')
if 'playwright' not in sys.modules:
    pw = types.ModuleType('playwright')
    pw_async = types.ModuleType('playwright.async_api')
    pw_async.async_playwright = lambda: None
    class _PWTimeout(Exception):
        pass
    pw_async.TimeoutError = _PWTimeout
    sys.modules['playwright'] = pw
    sys.modules['playwright.async_api'] = pw_async

from scrape_rosters import (parse_player_profile, pick_transfer_event,
                            apply_profile_to_row)

# --------------------------------------------------------------------------
# Tiny test harness \u2014 no pytest dependency, just clear pass/fail output.
# --------------------------------------------------------------------------
_state = {'failed': 0, 'total': 0}


def expect(actual, expected, label):
    _state['total'] += 1
    if actual == expected:
        print(f"  [OK ] {label}")
    else:
        _state['failed'] += 1
        print(f"  [FAIL] {label}: got={actual!r} expected={expected!r}")


def section(name):
    print(f"\n=== {name} ===")


# --------------------------------------------------------------------------
# Original behavior (regression coverage)
# --------------------------------------------------------------------------
HTML_TWO_TRANSFERS = """<html><body>
<div class="team-info-section"><header><h2>Auburn</h2></header></div>
<div class="commit-banner"><span>Alabama</span></div>

<section class="rankings-section">
  <h3 class="title">Transfer</h3>
  <div class="rank-block">88 (2023)</div>
  <span class="icon-starsolid yellow"></span>
  <span class="icon-starsolid yellow"></span>
  <span class="icon-starsolid yellow"></span>
  <ul>
    <li><b>OVR</b><strong>456</strong></li>
    <li><a href="?Position=WR"><b>WR</b><strong>34</strong></a></li>
  </ul>
</section>

<section class="rankings-section">
  <h3 class="title">Transfer</h3>
  <div class="rank-block">91 (2025)</div>
  <span class="icon-starsolid yellow"></span>
  <span class="icon-starsolid yellow"></span>
  <span class="icon-starsolid yellow"></span>
  <span class="icon-starsolid yellow"></span>
  <ul>
    <li><b>OVR</b><strong>123</strong></li>
    <li><a href="?Position=WR"><b>WR</b><strong>8</strong></a></li>
  </ul>
</section>
</body></html>"""

section("Test 1: profile with two transfer events (no break-after-first)")
p1 = parse_player_profile(HTML_TWO_TRANSFERS)
expect(p1['origin_team'], 'Auburn', 'origin team')
expect(p1['destination_team'], 'Alabama', 'destination team')
expect(len(p1['transfer_events']), 2, 'BOTH transfer events captured')
for season, expected_year in [(2022, None), (2023, 2023), (2024, 2023),
                              (2025, 2025), (2026, 2025)]:
    ev = pick_transfer_event(p1, season)
    expect(ev['year'] if ev else None, expected_year,
           f'  season={season} -> picks year={expected_year}')


section("Test 2: true freshman (no transfer section)")
HTML_NO_TRANSFER = """<html><body>
<section class="rankings-section">
  <h3 class="title">247Sports</h3>
  <div class="rank-block">94 (2024)</div>
  <ul><li><b>NATL</b><strong>50</strong></li></ul>
</section>
</body></html>"""
p2 = parse_player_profile(HTML_NO_TRANSFER)
expect(len(p2['transfer_events']), 0, 'no transfer events')
expect(pick_transfer_event(p2, 2024), None, 'pick returns None')


section("Test 3: rating extraction anchored to start of rank-block")
HTML_RATING_TRAP = """<html><body>
<section class="rankings-section">
  <h3 class="title">Transfer</h3>
  <div class="rank-block">88 (2024)</div>
  <ul><li><b>OVR</b><strong>200</strong></li></ul>
</section>
</body></html>"""
ev3 = parse_player_profile(HTML_RATING_TRAP)['transfer_events'][0]
expect(ev3['rating'], '88', 'leading rating only (NOT OVR=200)')
expect(ev3['year'], 2024, 'year')
expect(ev3['overall_rank'], '200', 'overall rank from list')


section("Test 4: JUCO + Transfer coexisting (Transfer wins for transfer_events)")
HTML_JUCO_AND_TRANSFER = """<html><body>
<section class="rankings-section">
  <h3 class="title">JUCO Recruit</h3>
  <div class="rank-block">85 (2024)</div>
  <ul><li><b>OVR</b><strong>10</strong></li></ul>
</section>
<section class="rankings-section">
  <h3 class="title">Transfer</h3>
  <div class="rank-block">87 (2024)</div>
  <ul><li><b>OVR</b><strong>50</strong></li></ul>
</section>
</body></html>"""
p4 = parse_player_profile(HTML_JUCO_AND_TRANSFER)
expect(len(p4['transfer_events']), 1, 'only Transfer goes into transfer_events')
expect(p4['transfer_events'][0]['rating'], '87', 'transfer rating, not JUCO')
expect(p4['prospect_event']['kind'], 'JUCO', 'JUCO captured as prospect_event')


section("Test 5: state rank vs position rank disambiguation via href")
HTML_STATE_RANK = """<html><body>
<section class="rankings-section">
  <h3 class="title">Transfer</h3>
  <div class="rank-block">90 (2024)</div>
  <ul>
    <li><b>OVR</b><strong>100</strong></li>
    <li><a href="?State=AL"><b>AL</b><strong>3</strong></a></li>
    <li><a href="?Position=QB"><b>QB</b><strong>5</strong></a></li>
  </ul>
</section>
</body></html>"""
ev5 = parse_player_profile(HTML_STATE_RANK)['transfer_events'][0]
expect(ev5['position'], 'QB', 'picked QB not AL')
expect(ev5['position_rank'], '5', 'position rank value')
expect(ev5['overall_rank'], '100', 'overall rank value')


# --------------------------------------------------------------------------
# Bug fix coverage
# --------------------------------------------------------------------------
HTML_HS_RECRUIT = """<html><body>
<section class="rankings-section">
  <h3 class="title">247Sports</h3>
  <div class="rank-block">94 (2022)</div>
  <span class="icon-starsolid yellow"></span>
  <span class="icon-starsolid yellow"></span>
  <span class="icon-starsolid yellow"></span>
  <span class="icon-starsolid yellow"></span>
  <ul>
    <li><b>NATL</b><strong>89</strong></li>
    <li><a href="?Position=WR"><b>WR</b><strong>5</strong></a></li>
  </ul>
</section>
</body></html>"""
section("Test 6 (Bug 1+2): prospect_event captured WITH national_rank")
p6 = parse_player_profile(HTML_HS_RECRUIT)
pe = p6['prospect_event']
expect(pe is not None, True, 'prospect_event populated (was None pre-fix)')
expect(pe['kind'], '247Sports', 'kind=247Sports')
expect(pe['rating'], '94', 'HS rating')
expect(pe['year'], 2022, 'HS class year')
expect(pe['national_rank'], '89', 'HS national rank (Bug 2 fix)')
expect(pe['position_rank'], '5', 'HS position rank')
expect(pe['position'], 'WR', 'HS position')
expect(pe['stars'], '4', 'HS stars')


section("Test 7: prospect tie-break prefers JUCO over 247Sports")
HTML_BOTH_PROSPECT = """<html><body>
<section class="rankings-section">
  <h3 class="title">247Sports</h3>
  <div class="rank-block">82 (2021)</div>
  <ul><li><b>NATL</b><strong>500</strong></li></ul>
</section>
<section class="rankings-section">
  <h3 class="title">JUCO Recruit</h3>
  <div class="rank-block">88 (2023)</div>
  <ul><li><b>OVR</b><strong>10</strong></li></ul>
</section>
</body></html>"""
p7 = parse_player_profile(HTML_BOTH_PROSPECT)
expect(p7['prospect_event']['kind'], 'JUCO', 'JUCO wins tie-break')
expect(p7['prospect_event']['stars'], 'JUCO', 'JUCO stars literal')
expect(p7['prospect_event']['year'], 2023, 'JUCO year')


section("Test 8 (Bug 4): undated transfer event still picked up")
HTML_UNDATED = """<html><body>
<section class="rankings-section">
  <h3 class="title">Transfer</h3>
  <div class="rank-block">85</div>
  <ul>
    <li><b>OVR</b><strong>250</strong></li>
    <li><a href="?Position=DB"><b>DB</b><strong>20</strong></a></li>
  </ul>
</section>
</body></html>"""
p8 = parse_player_profile(HTML_UNDATED)
expect(p8['transfer_events'][0]['year'], None, 'event captured w/ year=None')
ev8 = pick_transfer_event(p8, 2024)
expect(ev8 is not None, True, 'pick uses undated fallback (was None pre-fix)')
expect(ev8['rating'], '85', 'undated rating')
expect(ev8['overall_rank'], '250', 'undated overall rank')


section("Test 9 (Bug 4): dated event STILL preferred when both present")
HTML_MIXED = """<html><body>
<section class="rankings-section">
  <h3 class="title">Transfer</h3>
  <div class="rank-block">85</div>
  <ul><li><b>OVR</b><strong>250</strong></li></ul>
</section>
<section class="rankings-section">
  <h3 class="title">Transfer</h3>
  <div class="rank-block">90 (2023)</div>
  <ul><li><b>OVR</b><strong>100</strong></li></ul>
</section>
</body></html>"""
ev9 = pick_transfer_event(parse_player_profile(HTML_MIXED), 2024)
expect(ev9['year'], 2023, 'dated 2023 preferred over undated')


section("Test 10 (Bug 3): apply_profile_to_row writes hs_* fields")
row_freshman = {}
p_freshman = parse_player_profile(HTML_HS_RECRUIT)
p_freshman['fetch_status'] = 'ok'
apply_profile_to_row(row_freshman, p_freshman, 2024)
expect(row_freshman.get('profile_scraped'), 'ok_no_transfer', 'status')
expect(row_freshman.get('hs_class_year'), '2022', 'hs_class_year')
expect(row_freshman.get('hs_rating'), '94', 'hs_rating')
expect(row_freshman.get('hs_national_rank'), '89', 'hs_national_rank')
expect(row_freshman.get('hs_position'), 'WR', 'hs_position')
expect(row_freshman.get('hs_stars'), '4', 'hs_stars')
expect(row_freshman.get('hs_section_kind'), '247Sports', 'hs_section_kind')
expect(row_freshman.get('transfer_rating', ''), '', 'transfer fields blank')


HTML_FULL = """<html><body>
<div class="team-info-section"><header><h2>Auburn</h2></header></div>
<div class="commit-banner"><span>Alabama</span></div>
<section class="rankings-section">
  <h3 class="title">247Sports</h3>
  <div class="rank-block">92 (2022)</div>
  <span class="icon-starsolid yellow"></span>
  <span class="icon-starsolid yellow"></span>
  <span class="icon-starsolid yellow"></span>
  <span class="icon-starsolid yellow"></span>
  <ul>
    <li><b>NATL</b><strong>120</strong></li>
    <li><a href="?Position=RB"><b>RB</b><strong>8</strong></a></li>
  </ul>
</section>
<section class="rankings-section">
  <h3 class="title">Transfer</h3>
  <div class="rank-block">88 (2024)</div>
  <ul>
    <li><b>OVR</b><strong>200</strong></li>
    <li><a href="?Position=RB"><b>RB</b><strong>15</strong></a></li>
  </ul>
</section>
</body></html>"""
section("Test 11: full profile (HS + transfer) populates BOTH groups")
row_full = {}
p_full = parse_player_profile(HTML_FULL)
p_full['fetch_status'] = 'ok'
apply_profile_to_row(row_full, p_full, 2024)
expect(row_full.get('profile_scraped'), 'ok', 'status=ok')
expect(row_full.get('transfer_origin_team'), 'Auburn', 'origin')
expect(row_full.get('transfer_destination_team'), 'Alabama', 'destination')
expect(row_full.get('transfer_class_year'), '2024', 'transfer year')
expect(row_full.get('transfer_rating'), '88', 'transfer rating')
expect(row_full.get('hs_class_year'), '2022', 'hs year')
expect(row_full.get('hs_rating'), '92', 'hs rating')
expect(row_full.get('hs_national_rank'), '120', 'hs NATL')
expect(row_full.get('hs_position_rank'), '8', 'hs pos rank')


section("Test 12 (regression): transfer events still get OVR (not NATL)")
ev_t = parse_player_profile(HTML_FULL)['transfer_events'][0]
expect(ev_t['overall_rank'], '200', 'transfer OVR captured')
expect(ev_t['national_rank'], '', 'transfer NATL empty (no NATL in section)')


# --------------------------------------------------------------------------
print("\n" + "=" * 60)
total = _state['total']
failed = _state['failed']
passed = total - failed
print(f"{passed}/{total} assertions passed"
      + ("" if failed == 0 else f" \u2014 {failed} FAILED"))
sys.exit(0 if failed == 0 else 1)
