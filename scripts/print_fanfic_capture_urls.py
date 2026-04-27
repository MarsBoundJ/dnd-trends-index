"""
Arcane Analytics — print deep-link URLs for fanfic-crossover capture
(Stage 4 of the community_reception multi-source composite, Apr 27, 2026).

Reads seed_fanfic_canonical_tags.py and prints a flat list of:
  <IP name>  AO3: <url>  FFN: <url>

Phil's manual data-collection workflow:
  1. python scripts/print_fanfic_capture_urls.py > /tmp/capture-urls.txt
  2. Open the file, click each AO3 link, AO3 results page loads
  3. Click the AO3 bookmarklet from the bookmark bar
  4. Confirm the count + IP attribution, modal POSTs to bouncer
  5. Repeat for the FFN link
  6. Move to next IP

Each URL includes a `&_arcane_ip=...` marker so the bookmarklet auto-
attributes the captured count to the right seed-list IP — no dropdown
or manual typing needed.

Future TODO: replace this with a /admin/fanfic-capture page that
renders the same data as a clickable table with checkmarks for
already-captured IPs. CLI for the v1 ship to keep Stage 4 scoped.

Usage:
    python scripts/print_fanfic_capture_urls.py
    python scripts/print_fanfic_capture_urls.py --platform ao3   # AO3 only
    python scripts/print_fanfic_capture_urls.py --platform ffn   # FFN only
    python scripts/print_fanfic_capture_urls.py --html           # HTML page
"""

from __future__ import annotations

import argparse
import urllib.parse

from seed_fanfic_canonical_tags import (
    MAPPINGS, D_AND_D_AO3, D_AND_D_FFN_ID,
)


def ao3_url(canonical_tag: str, ip_name: str) -> str:
    """Build AO3 D&D-tag-page URL filtered by an additional IP fandom tag.

    Pattern: /tags/[D&D tag]/works?work_search[other_tag_names]=[IP tag]

    Why this pattern instead of /works/search?:
    - /works/search?work_search[tag_names]=... was the original attempt
      but AO3 silently ignores `tag_names` (not a real param). Bookmarklet
      saw 15M counts (site-wide AO3 works) on first smoke test. We
      switched to the canonical tag-page route + AO3's actual filter
      param `other_tag_names` for the secondary tag.
    - /tags/[NAME]/works is also explicitly allowed by AO3 robots.txt
      for general User-agent (vs /works/search? which is Disallow).
      Even though humans clicking links in browsers aren't bound by
      robots.txt, using an allowed URL is cleaner ethics-wise.
    """
    dnd_path = urllib.parse.quote(D_AND_D_AO3, safe='')
    qs = urllib.parse.urlencode({
        'work_search[other_tag_names]': canonical_tag,
        '_arcane_ip': ip_name,
    })
    return f'https://archiveofourown.org/tags/{dnd_path}/works?{qs}'


def ffn_url(ffn_id: int | None, ip_name: str) -> str | None:
    """Build FFN crossover URL for D&D + IP. Returns None if FFN ID unknown."""
    if not ffn_id:
        return None
    # FFN crossover URL pattern: /[A]-and-[B]-Crossovers/[ID_A]/[ID_B]/
    # We build it generically; the slug parts FFN cares about are the IDs.
    qs = urllib.parse.urlencode({'_arcane_ip': ip_name})
    # Use IP_FIRST format (FFN's own URL convention varies — both orders work)
    return (
        f'https://www.fanfiction.net/'
        f'IP-and-Dungeons-and-Dragons-Crossovers/'
        f'{ffn_id}/{D_AND_D_FFN_ID}/?{qs}'
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--platform', choices=['ao3', 'ffn', 'all'], default='all',
        help='Which platform to print URLs for.',
    )
    parser.add_argument(
        '--html', action='store_true',
        help='Render as a clickable HTML page (paste output into a .html file).',
    )
    args = parser.parse_args()

    if args.html:
        print('<!DOCTYPE html><html><head><meta charset="utf-8">')
        print('<title>Arcane: Fanfic Capture URLs</title>')
        print('<style>')
        print('body{font-family:-apple-system,sans-serif;max-width:900px;')
        print('  margin:2em auto;padding:0 1em;background:#0d0d1a;color:#e0e0ff}')
        print('h1{color:#e87722}h2{color:#a78bfa;margin-top:1.5em}')
        print('table{width:100%;border-collapse:collapse;margin:1em 0}')
        print('th,td{text-align:left;padding:.5em;border-bottom:1px solid #2a2a4a}')
        print('a{color:#60a5fa;text-decoration:none}')
        print('a:hover{text-decoration:underline}')
        print('.todo{color:#f97316;font-style:italic}')
        print('</style></head><body>')
        print('<h1>Fanfic Capture URLs</h1>')
        print(f'<p>Click each link in turn, then click the bookmarklet from your bookmark bar on the loaded page.</p>')
        print('<h2>By IP</h2>')
        print('<table><tr><th>IP</th><th>AO3</th><th>FFN</th></tr>')

    for m in MAPPINGS:
        ao3 = ao3_url(m.ao3_tag, m.ip_name)
        ffn = ffn_url(m.ffn_id, m.ip_name)

        if args.html:
            ao3_cell = f'<a href="{ao3}" target="_blank">AO3</a>' if args.platform != 'ffn' else ''
            if args.platform != 'ao3':
                ffn_cell = (
                    f'<a href="{ffn}" target="_blank">FFN</a>' if ffn
                    else '<span class="todo">FFN ID TODO</span>'
                )
            else:
                ffn_cell = ''
            print(f'<tr><td>{m.ip_name}</td><td>{ao3_cell}</td><td>{ffn_cell}</td></tr>')
        else:
            print(f'\n=== {m.ip_name} ===')
            if args.platform != 'ffn':
                print(f'  AO3: {ao3}')
            if args.platform != 'ao3':
                if ffn:
                    print(f'  FFN: {ffn}')
                else:
                    print(f'  FFN: (FFN ID TODO — visit https://www.fanfiction.net/'
                          f'Dungeons-and-Dragons-Crossovers/{D_AND_D_FFN_ID}/0/ '
                          f'and find {m.ip_name})')

    if args.html:
        print('</table></body></html>')


if __name__ == '__main__':
    main()
