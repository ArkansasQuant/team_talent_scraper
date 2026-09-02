"""
FBS team roster URL registry.

Each entry maps a canonical team name to the 247sports roster URL pattern.
The URL structure is:
    https://247sports.com/college/{school_slug}/team/{team_slug}-football-{team_id}/roster/

and we append `?year={season}` to get per-season rosters.

247 also serves the same roster at two other URL forms, which the scraper
uses as fallbacks when the primary form redirects to a different season
(observed for the current season right after 247 publishes it):
    https://247sports.com/college/{school_slug}/team/{team_slug}/roster/          (current season, no year)
    https://247sports.com/team/{team_slug}/roster/?year={season}                  (site-agnostic)

This list is hand-curated and verified against 247sports URLs. Teams that
joined FBS mid-window have a `first_fbs_year` field so the scraper knows
to skip earlier seasons (they'll 404 or return FCS rosters).

Derived from the FBS membership map in the PFF↔247 matching pipeline.
"""

# Format: canonical_team: (school_slug, team_slug, team_id, first_fbs_year)
TEAM_URLS = {
    # SEC
    'Alabama':           ('alabama', 'alabama-crimson-tide-football-164', 164, 2018),
    'Arkansas':          ('arkansas', 'arkansas-razorbacks-football-166', 166, 2018),
    'Auburn':            ('auburn', 'auburn-tigers-football-168', 168, 2018),
    'Florida':           ('florida', 'florida-gators-football-170', 170, 2018),
    'Georgia':           ('georgia', 'georgia-bulldogs-football-172', 172, 2018),
    'Kentucky':          ('kentucky', 'kentucky-wildcats-football-174', 174, 2018),
    'LSU':               ('lsu', 'lsu-tigers-football-176', 176, 2018),
    'Mississippi State': ('mississippi-state', 'mississippi-state-bulldogs-football-180', 180, 2018),
    'Missouri':          ('missouri', 'missouri-tigers-football-458', 458, 2018),
    'Ole Miss':          ('ole-miss', 'ole-miss-rebels-football-178', 178, 2018),
    'South Carolina':    ('south-carolina', 'south-carolina-gamecocks-football-182', 182, 2018),
    'Tennessee':         ('tennessee', 'tennessee-volunteers-football-184', 184, 2018),
    'Texas A&M':         ('texas-am', 'texas-am-aggies-football-459', 459, 2018),
    'Vanderbilt':        ('vanderbilt', 'vanderbilt-commodores-football-186', 186, 2018),
    'Oklahoma':          ('oklahoma', 'oklahoma-sooners-football-39', 39, 2018),
    'Texas':             ('texas', 'texas-longhorns-football-43', 43, 2018),

    # Big Ten
    'Illinois':          ('illinois', 'illinois-fighting-illini-football-65', 65, 2018),
    'Indiana':           ('indiana', 'indiana-hoosiers-football-67', 67, 2018),
    'Iowa':              ('iowa', 'iowa-hawkeyes-football-69', 69, 2018),
    'Maryland':          ('maryland', 'maryland-terrapins-football-11', 11, 2018),
    'Michigan':          ('michigan', 'michigan-wolverines-football-71', 71, 2018),
    'Michigan State':    ('michigan-state', 'michigan-state-spartans-football-73', 73, 2018),
    'Minnesota':         ('minnesota', 'minnesota-golden-gophers-football-75', 75, 2018),
    'Nebraska':          ('nebraska', 'nebraska-cornhuskers-football-37', 37, 2018),
    'Northwestern':      ('northwestern', 'northwestern-wildcats-football-77', 77, 2018),
    'Ohio State':        ('ohio-state', 'ohio-state-buckeyes-football-79', 79, 2018),
    'Penn State':        ('penn-state', 'penn-state-nittany-lions-football-81', 81, 2018),
    'Purdue':            ('purdue', 'purdue-boilermakers-football-83', 83, 2018),
    'Rutgers':           ('rutgers', 'rutgers-scarlet-knights-football-57', 57, 2018),
    'Wisconsin':         ('wisconsin', 'wisconsin-badgers-football-85', 85, 2018),
    'UCLA':              ('ucla', 'ucla-bruins-football-158', 158, 2018),
    'USC':               ('usc', 'usc-trojans-football-214', 214, 2018),
    'Oregon':            ('oregon', 'oregon-ducks-football-152', 152, 2018),
    'Washington':        ('washington', 'washington-huskies-football-160', 160, 2018),

    # ACC
    'Boston College':    ('boston-college', 'boston-college-eagles-football-1', 1, 2018),
    'Clemson':           ('clemson', 'clemson-tigers-football-3', 3, 2018),
    'Duke':              ('duke', 'duke-blue-devils-football-5', 5, 2018),
    'Florida State':     ('florida-state', 'florida-state-seminoles-football-7', 7, 2018),
    'Georgia Tech':      ('georgia-tech', 'georgia-tech-yellow-jackets-football-9', 9, 2018),
    'Louisville':        ('louisville', 'louisville-cardinals-football-53', 53, 2018),
    'Miami':             ('miami', 'miami-hurricanes-football-13', 13, 2018),
    'NC State':          ('north-carolina-state', 'nc-state-wolfpack-football-17', 17, 2018),
    'North Carolina':    ('north-carolina', 'north-carolina-tar-heels-football-15', 15, 2018),
    'Pittsburgh':        ('pittsburgh', 'pittsburgh-panthers-football-460', 460, 2018),
    'Syracuse':          ('syracuse', 'syracuse-orange-football-461', 461, 2018),
    'Virginia':          ('virginia', 'virginia-cavaliers-football-19', 19, 2018),
    'Virginia Tech':     ('virginia-tech', 'virginia-tech-hokies-football-21', 21, 2018),
    'Wake Forest':       ('wake-forest', 'wake-forest-demon-deacons-football-23', 23, 2018),
    'Notre Dame':        ('notre-dame', 'notre-dame-fighting-irish-football-109', 109, 2018),
    'California':        ('california', 'california-golden-bears-football-150', 150, 2018),
    'Stanford':          ('stanford', 'stanford-cardinal-football-156', 156, 2018),
    'SMU':               ('smu', 'smu-mustangs-football-426', 426, 2018),

    # Big 12
    'Baylor':            ('baylor', 'baylor-bears-football-25', 25, 2018),
    'BYU':               ('byu', 'byu-cougars-football-134', 134, 2018),
    'Cincinnati':        ('cincinnati', 'cincinnati-bearcats-football-49', 49, 2018),
    'Houston':           ('houston', 'houston-cougars-football-89', 89, 2018),
    'Iowa State':        ('iowa-state', 'iowa-state-cyclones-football-29', 29, 2018),
    'Kansas':            ('kansas', 'kansas-jayhawks-football-31', 31, 2018),
    'Kansas State':      ('kansas-state', 'kansas-state-wildcats-football-33', 33, 2018),
    'Oklahoma State':    ('oklahoma-state', 'oklahoma-state-cowboys-football-41', 41, 2018),
    'TCU':               ('tcu', 'tcu-horned-frogs-football-456', 456, 2018),
    'Texas Tech':        ('texas-tech', 'texas-tech-red-raiders-football-47', 47, 2018),
    'UCF':               ('central-florida', 'ucf-knights-football-103', 103, 2018),
    'West Virginia':     ('west-virginia', 'west-virginia-mountaineers-football-457', 457, 2018),
    'Arizona':           ('arizona', 'arizona-wildcats-football-146', 146, 2018),
    'Arizona State':     ('arizona-state', 'arizona-state-sun-devils-football-148', 148, 2018),
    'Colorado':          ('colorado', 'colorado-buffaloes-football-27', 27, 2018),
    'Utah':              ('utah', 'utah-utes-football-144', 144, 2018),

    # American (AAC)
    'East Carolina':     ('east-carolina', 'east-carolina-pirates-football-87', 87, 2018),
    'Memphis':           ('memphis', 'memphis-tigers-football-93', 93, 2018),
    'Navy':              ('navy', 'navy-midshipmen-football-108', 108, 2018),
    'Temple':            ('temple', 'temple-owls-football-127', 127, 2018),
    'Tulane':            ('tulane', 'tulane-green-wave-football-97', 97, 2018),
    'Tulsa':             ('tulsa', 'tulsa-golden-hurricane-football-99', 99, 2018),
    'Charlotte':         ('charlotte', 'charlotte-49ers-football-474', 474, 2018),
    'Florida Atlantic':  ('florida-atlantic', 'florida-atlantic-owls-football-443', 443, 2018),
    'North Texas':       ('north-texas', 'north-texas-mean-green-football-196', 196, 2018),
    'Rice':              ('rice', 'rice-owls-football-428', 428, 2018),
    'UAB':               ('alabama-birmingham', 'uab-blazers-football-101', 101, 2018),
    'UTSA':              ('utsa', 'utsa-roadrunners-football-291', 291, 2018),
    'South Florida':     ('south-florida', 'south-florida-bulls-football-59', 59, 2018),
    'Army':              ('army', 'army-black-knights-football-107', 107, 2018),

    # Conference USA
    'FIU':               ('florida-international', 'fiu-panthers-football-441', 441, 2018),
    'Louisiana Tech':    ('louisiana-tech', 'louisiana-tech-bulldogs-football-208', 208, 2018),
    'Middle Tennessee':  ('middle-tennessee-state', 'middle-tennessee-blue-raiders-football-194', 194, 2018),
    'New Mexico State':  ('new-mexico-state', 'new-mexico-state-aggies-football-210', 210, 2018),
    'UTEP':              ('utep', 'utep-miners-football-105', 105, 2018),
    'Western Kentucky':  ('western-kentucky', 'western-kentucky-hilltoppers-football-200', 200, 2018),
    'Jacksonville State':('jacksonville-state', 'jacksonville-state-gamecocks-football-332', 332, 2023),
    'Liberty':           ('liberty', 'liberty-flames-football-268', 268, 2018),
    'Sam Houston':       ('sam-houston-state', 'sam-houston-bearkats-football-364', 364, 2023),
    'Kennesaw State':    ('kennesaw-state', 'kennesaw-state-owls-football-754', 754, 2024),
    'Delaware':          ('delaware', 'delaware-fightin-blue-hens-football-755', 755, 2025),
    'Missouri State':    ('missouri-state', 'missouri-state-bears-football-313', 313, 2025),

    # MAC
    'Akron':             ('akron', 'akron-zips-football-111', 111, 2018),
    'Ball State':        ('ball-state', 'ball-state-cardinals-football-113', 113, 2018),
    'Bowling Green':     ('bowling-green', 'bowling-green-falcons-football-115', 115, 2018),
    'Buffalo':           ('buffalo', 'buffalo-bulls-football-116', 116, 2018),
    'Central Michigan':  ('central-michigan', 'central-michigan-chippewas-football-118', 118, 2018),
    'Eastern Michigan':  ('eastern-michigan', 'eastern-michigan-eagles-football-448', 448, 2018),
    'Kent State':        ('kent-state', 'kent-state-golden-flashes-football-119', 119, 2018),
    'Miami (OH)':        ('miami-ohio', 'miami-oh-redhawks-football-121', 121, 2018),
    'Ohio':              ('ohio', 'ohio-bobcats-football-125', 125, 2018),
    'Toledo':            ('toledo', 'toledo-rockets-football-128', 128, 2018),
    'Western Michigan':  ('western-michigan', 'western-michigan-broncos-football-130', 130, 2018),
    'Massachusetts':     ('massachusetts', 'umass-minutemen-football-475', 475, 2018),
    'Northern Illinois': ('northern-illinois', 'northern-illinois-huskies-football-123', 123, 2018),
    'Sacramento State':  ('sacramento-state', 'sacramento-state-hornets-football-263', 263, 2026),   # listed in 247's 2026 FBS dropdown (MAC)

    # Mountain West
    'Air Force':         ('air-force', 'air-force-falcons-football-132', 132, 2018),
    'Boise State':       ('boise-state', 'boise-state-broncos-football-202', 202, 2018),
    'Colorado State':    ('colorado-state', 'colorado-state-rams-football-135', 135, 2018),
    'Fresno State':      ('fresno-state', 'fresno-state-bulldogs-football-440', 440, 2018),
    'Hawaii':            ('hawaii', 'hawaii-rainbow-warriors-football-204', 204, 2018),
    'Nevada':            ('nevada', 'nevada-wolf-pack-football-438', 438, 2018),
    'New Mexico':        ('new-mexico', 'new-mexico-lobos-football-137', 137, 2018),
    'San Diego State':   ('san-diego-state', 'san-diego-state-aztecs-football-139', 139, 2018),
    'San Jose State':    ('san-jose-state', 'san-jose-state-spartans-football-212', 212, 2018),
    'UNLV':              ('unlv', 'unlv-rebels-football-142', 142, 2018),
    'Utah State':        ('utah-state', 'utah-state-aggies-football-439', 439, 2018),
    'Wyoming':           ('wyoming', 'wyoming-cowboys-football-430', 430, 2018),
    'North Dakota State':('north-dakota-state', 'north-dakota-state-bison-football-314', 314, 2026),   # listed in 247's 2026 FBS dropdown (MWC)

    # Sun Belt
    'Appalachian State': ('appalachian-state', 'appalachian-state-mountaineers-football-350', 350, 2018),
    'Arkansas State':    ('arkansas-state', 'arkansas-state-red-wolves-football-188', 188, 2018),
    'Coastal Carolina':  ('coastal-carolina', 'coastal-carolina-chanticleers-football-266', 266, 2018),
    'Georgia Southern':  ('georgia-southern', 'georgia-southern-eagles-football-355', 355, 2018),
    'Georgia State':     ('georgia-state', 'georgia-state-panthers-football-288', 288, 2018),
    'James Madison':     ('james-madison', 'james-madison-dukes-football-273', 273, 2022),
    'Louisiana':         ('louisiana', 'louisiana-ragin-cajuns-football-190', 190, 2018),
    'Louisiana-Monroe':  ('louisiana-monroe', 'louisiana-monroe-warhawks-football-192', 192, 2018),
    'Marshall':          ('marshall', 'marshall-thundering-herd-football-91', 91, 2018),
    'Old Dominion':      ('old-dominion', 'old-dominion-monarchs-football-277', 277, 2018),
    'South Alabama':     ('south-alabama', 'south-alabama-jaguars-football-289', 289, 2018),
    'Southern Miss':     ('southern-mississippi', 'southern-miss-golden-eagles-football-95', 95, 2018),
    'Texas State':       ('texas-state', 'texas-state-bobcats-football-290', 290, 2018),
    'Troy':              ('troy', 'troy-trojans-football-198', 198, 2018),

    # Independents & moved-up
    'Oregon State':      ('oregon-state', 'oregon-state-beavers-football-154', 154, 2018),
    'Washington State':  ('washington-state', 'washington-state-cougars-football-162', 162, 2018),
    'UConn':             ('uconn', 'uconn-huskies-football-51', 51, 2018),
    'Idaho':             ('idaho', 'idaho-vandals-football-206', 206, 2024),   # moved back to FBS
}

def team_url(canonical_team, season):
    """Build the roster URL for a given team and season."""
    if canonical_team not in TEAM_URLS:
        raise KeyError(f"Unknown team: {canonical_team}")
    school_slug, team_slug, team_id, first_fbs = TEAM_URLS[canonical_team]
    return f"https://247sports.com/college/{school_slug}/team/{team_slug}/roster/?year={season}"

def roster_url_candidates(canonical_team, season):
    """All URL forms that can serve the (team, season) roster, best first.

    The scraper tries them in order and accepts the first page whose
    <h1>/<title> season matches `season` and that contains roster rows.
    """
    if canonical_team not in TEAM_URLS:
        raise KeyError(f"Unknown team: {canonical_team}")
    school_slug, team_slug, team_id, first_fbs = TEAM_URLS[canonical_team]
    return [
        f"https://247sports.com/college/{school_slug}/team/{team_slug}/roster/?year={season}",
        f"https://247sports.com/college/{school_slug}/team/{team_slug}/roster/",
        f"https://247sports.com/team/{team_slug}/roster/?year={season}",
    ]

def is_fbs_in_year(canonical_team, season):
    if canonical_team not in TEAM_URLS:
        return False
    _, _, _, first_fbs = TEAM_URLS[canonical_team]
    return season >= first_fbs

def all_teams():
    return sorted(TEAM_URLS.keys())
