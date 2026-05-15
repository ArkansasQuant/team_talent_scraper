"""One-off diagnostic: fetch a known 2018-era player profile and dump the
exact HTML structure of its rankings sections, rank-block, stars, and any
draft-related elements. Output goes to debug_profile_output.txt.
"""
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# Five test profiles — mix of a 2018 walk-on, a 2018 4-star, and 3 NFL draftees
TEST_URLS = [
    "https://247sports.com/player/marvis-brown-35575/college-188/",   # 2018 walk-on (timed out in your run)
    "https://247sports.com/player/jameson-williams-46040514/college-239218/",  # 2021 Alabama (transferred)
    "https://247sports.com/player/quinshon-judkins-46106303/",        # recent transfer
    "https://247sports.com/player/will-howard-46050763/",             # recent draftee
    "https://247sports.com/player/aiden-fisher-46104017/",            # recent transfer
]

async def inspect(url):
    print("=" * 100)
    print(f"URL: {url}")
    print("=" * 100)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context()
        page = await ctx.new_page()
        # block heavy assets
        await page.route("**/*.{png,jpg,jpeg,gif,svg,webp,mp4,woff,woff2,css}",
                          lambda r: r.abort())
        try:
            await page.goto(url, timeout=45000, wait_until="domcontentloaded")
            await asyncio.sleep(3)
            html = await page.content()
        except Exception as e:
            print(f"FAILED: {e}")
            await browser.close()
            return
        await browser.close()

    soup = BeautifulSoup(html, 'lxml')

    # 1. Section titles
    print("\n--- rankings-section titles ---")
    sections = soup.select('section.rankings-section')
    print(f"Found {len(sections)} sections")
    for s in sections:
        t = s.select_one('h3.title, h3, h2, h4')
        print(f"  title element: {t.name if t else 'none'}, text: {(t.get_text(strip=True) if t else '')!r}")

    # 2. Rank block content per section
    for i, s in enumerate(sections):
        title = s.select_one('h3, h2, h4')
        ttext = title.get_text(strip=True) if title else '?'
        print(f"\n--- section #{i}: {ttext!r} ---")
        rb = s.select_one('.rank-block')
        if rb:
            print(f"  .rank-block found, text: {rb.get_text(' ', strip=True)[:200]!r}")
            print(f"  .rank-block raw HTML (first 500 chars): {str(rb)[:500]}")
        else:
            print(f"  NO .rank-block found")
            # what's in this section?
            print(f"  section direct children: {[c.name for c in s.children if hasattr(c,'name') and c.name]}")
            print(f"  section text (first 300): {s.get_text(' ', strip=True)[:300]!r}")
        # stars
        stars = s.select('span.icon-starsolid.yellow')
        stars_any = s.select('[class*="star"]')
        print(f"  span.icon-starsolid.yellow count: {len(stars)}")
        print(f"  any [class*=star] count: {len(stars_any)}")
        if stars_any[:3]:
            for st in stars_any[:3]:
                print(f"    {st.name} class={st.get('class')!r}")
        # li rows
        lis = s.select('li')
        print(f"  <li> count in section: {len(lis)}")
        for li in lis[:5]:
            b = li.find('b')
            strong = li.find('strong')
            a = li.find('a')
            print(f"    li: <b>={b.get_text(strip=True) if b else None!r} "
                  f"<strong>={strong.get_text(strip=True) if strong else None!r} "
                  f"href={a.get('href','')[:80] if a else None!r}")

    # 3. Anything with 'draft' in class or text
    print(f"\n--- DRAFT mentions on page ---")
    for el in soup.select('[class*="draft"], [class*="Draft"]'):
        txt = el.get_text(' ', strip=True)[:200]
        if txt:
            print(f"  <{el.name} class={el.get('class')}>: {txt!r}")
    # also look for "NFL Draft" in any text
    full_text = soup.get_text(' ', strip=True)
    if 'NFL Draft' in full_text:
        idx = full_text.find('NFL Draft')
        print(f"\n  'NFL Draft' found at char {idx}, context:")
        print(f"  ...{full_text[max(0,idx-150):idx+150]!r}...")

    # 4. team-info-section and commit-banner
    print(f"\n--- team-info / commit ---")
    tinfo = soup.select_one('.team-info-section header h2')
    print(f"  .team-info-section header h2: {tinfo.get_text(strip=True) if tinfo else 'NOT FOUND'!r}")
    commit = soup.select_one('.commit-banner span')
    print(f"  .commit-banner span: {commit.get_text(strip=True) if commit else 'NOT FOUND'!r}")

async def main():
    for url in TEST_URLS:
        await inspect(url)
        print("\n\n")

if __name__ == "__main__":
    asyncio.run(main())
