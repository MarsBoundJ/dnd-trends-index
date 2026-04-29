/*
 * Arcane Analytics — Bookmarklet registry (Step 12).
 *
 * The minified `javascript:` payloads live in `scripts/*.txt` at the repo
 * root — they're maintained there because the same files are installable
 * from the legacy HTML frontend too. This module reads them at module
 * load (server-side) and exposes them as a typed registry so the
 * Harvesting Cockpit page can render one card per bookmarklet.
 *
 * `"server-only"` guards the module from ever being bundled into the
 * client — `fs` reads won't work in the browser and the payloads can
 * be ~14KB each, no need to ship them with the HTML.
 */

import "server-only"

import fs from "node:fs"
import path from "node:path"

export interface Bookmarklet {
  id: string
  /** Human-readable name shown on the card. */
  name: string
  /** Host the bookmarklet is intended to run on. */
  targetHost: string
  /** What it does in one sentence. */
  description: string
  /** Short "how to use after installing" note. */
  usage: string
  /** Raw `javascript:...` URL — dropped into an <a href> for drag-install. */
  href: string
}

// Script filename under `<repo>/scripts/`. Server-side resolution so we
// tolerate whatever CWD the Next build happens to be run from — process.cwd()
// in Next is the `arcane/` folder, hence the `../` to reach the repo root.
function readBookmarklet(filename: string): string {
  const filePath = path.resolve(process.cwd(), "..", "scripts", filename)
  return fs.readFileSync(filePath, "utf8").trim()
}

/**
 * Read a raw `.js` file (an IIFE with no `javascript:` prefix) from
 * `<repo>/utils/`, URL-encode it, and prepend `javascript:` so it's
 * installable as a browser bookmark. Used for the DMs Guild / DTRPG
 * bookmarklet that lives in utils/ rather than scripts/ — legacy
 * frontend convention we keep for now.
 */
function wrapBookmarkletFromJs(utilsFilename: string): string {
  const filePath = path.resolve(process.cwd(), "..", "utils", utilsFilename)
  const raw = fs.readFileSync(filePath, "utf8").trim()
  return `javascript:${encodeURIComponent(raw)}`
}

// Lazy-memoized so we don't hit the disk on every render. Exported as a
// function rather than a top-level const so Turbopack hot-reload doesn't
// race with file-system ordering.
let cached: Bookmarklet[] | null = null

export function getBookmarklets(): Bookmarklet[] {
  if (cached) return cached
  cached = [
    {
      id: "amazon",
      name: "Amazon Harvester",
      targetHost: "amazon.com",
      description:
        "Scrapes Amazon TTRPG best-seller, movers & shakers, new release, and most-wished-for lists into the catalog pipeline. Runs 10 category fetches in sequence.",
      usage:
        "Drag the pill to your bookmarks bar. Visit any Amazon product page, then click the bookmark — it will fetch the 10 configured list pages and post to Bouncer.",
      href: readBookmarklet("amazon_bookmarklet.txt"),
    },
    {
      id: "kickstarter",
      name: "Kickstarter Harvester",
      targetHost: "kickstarter.com",
      description:
        "Scrapes D&D-adjacent Kickstarter projects (Popular / Newest / Ending Soon sorts, keyword-filtered) and sends them to the Bouncer ingest route.",
      usage:
        "Drag the pill to your bookmarks bar. Visit kickstarter.com while signed in, then click the bookmark.",
      href: readBookmarklet("kickstarter_bookmarklet.txt"),
    },
    {
      id: "dmsguild-dtrpg",
      name: "DMsGuild / DTRPG Incursion",
      targetHost: "dmsguild.com · drivethrurpg.com",
      description:
        "Shared bookmarklet for DMs Guild and DriveThruRPG. Scrapes product links (including metal-tier shelf context when on metal.php) and POSTs to Bouncer. Auto-detects which site you're on by hostname.",
      usage:
        "Drag the pill to your bookmarks bar. Works on either dmsguild.com or drivethrurpg.com — click it on a best-seller or metal-tier page and it will harvest every product link on the page.",
      href: wrapBookmarkletFromJs("dmsguild_incursion_mini.js"),
    },
    {
      id: "ao3-crossover",
      name: "AO3 D&D-Crossover Capture",
      targetHost: "archiveofourown.org",
      description:
        "Reads the visible work count from an AO3 search results page (e.g. 'Dungeons & Dragons (Roleplaying Game)' filtered to '+ The Lord of the Rings — All Media Types') and POSTs to Bouncer. Stage 4 of community_reception. Captures organic crossover demand — fans don't write 4,500 Percy Jackson × D&D fic for marketing reasons.",
      usage:
        "Drag the pill to your bookmarks bar. Navigate to the AO3 search URL for the IP × D&D combination (the URL can include `&_arcane_ip=Name` so the bookmarklet auto-attributes the count to a seed-list IP — otherwise it prompts). Click the bookmark on the loaded results page; confirm the count and IP in the modal.",
      href: readBookmarklet("ao3_bookmarklet.txt"),
    },
    {
      id: "ffn-crossover",
      name: "FFN D&D-Crossover Capture",
      targetHost: "fanfiction.net",
      description:
        "Reads the story count from an FFN crossover page (e.g. /Lord-of-the-Rings-and-Dungeons-and-Dragons-Crossovers/382/1116/) and POSTs to Bouncer. Companion to the AO3 capture for cross-platform reception triangulation.",
      usage:
        "Drag the pill to your bookmarks bar. Navigate to FFN's D&D crossover hub at /Dungeons-and-Dragons-Crossovers/1116/0/, click through to the IP-specific crossover page (e.g. Lord of the Rings × D&D), then click the bookmark. The bookmarklet derives the story count from FFN's page-of-N pagination + per-page row count.",
      href: readBookmarklet("ffn_bookmarklet.txt"),
    },
    {
      id: "ddb-homebrew",
      name: "DDB Homebrew Capture",
      targetHost: "dndbeyond.com",
      description:
        "Stage 6a of community_reception. BULK MODE (default): land on any dndbeyond.com page, click once, bookmarklet sequentially fetches /homebrew/<section>?filter-name=<IP> for all 40 priority IPs × 5 priority sections = 200 captures using your DDB session cookies (Amazon-bookmarklet pattern). Parses .list-row[data-slug] containers, POSTs each to /system/homebrew/ingest-ddb. ~5-7 minutes per full run. Skips already-captured combinations using server-side timestamps + localStorage sent log. MANUAL MODE: searchable dropdown for ad-hoc one-off captures.",
      usage:
        "Drag the pill to your bookmarks bar. Open ANY dndbeyond.com page while signed in (or browse a homebrew section). Click the bookmark. Bulk-mode plan screen shows already-done vs pending counts; click 'Capture pending'. Live progress bar + feed of saved/empty/failed entries. Pause/abort button if you need to stop. When complete, refresh-plan button updates the counts.",
      href: readBookmarklet("ddb_homebrew_bookmarklet.txt"),
    },
  ]
  return cached
}
