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
  ]
  return cached
}
