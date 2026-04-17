/*
 * Arcane Analytics — Atlas section registry (Step 10).
 *
 * Single source of truth for every destination the Atlas surfaces. Keeps
 * copy and status out of the component so a new page can join the site
 * map by adding one entry here — no component edits required.
 *
 * Decision (Step 10): Variant B — the Atlas surfaces every discoverable
 * page AND the forward-looking roadmap slots from FRONTEND_DESIGN_SPEC.md
 * §3.6. This over-counts the spec's "six sections" but delivers on the
 * spec's stated *intent* ("doubles as onboarding for first-time users"),
 * which requires every real page to be present in the site map.
 *
 * Planned tiles render disabled with a tooltip; they double as a visible
 * public roadmap without forcing us to stand up placeholder routes.
 */

import {
  Home,
  TrendingUp,
  Newspaper,
  Backpack,
  Gem,
  Gamepad2,
  Microscope,
  BookOpen,
  type LucideIcon,
} from "lucide-react"

export type AtlasSectionStatus = "active" | "planned"

export interface AtlasSection {
  id: string
  title: string
  /** One-line Sage-voiced blurb — analytical, no marketing-speak. */
  description: string
  icon: LucideIcon
  /** Next.js route. Omit for planned sections. */
  route?: string
  status: AtlasSectionStatus
  /** Optional note shown under planned sections (e.g. "Post-launch"). */
  plannedNote?: string
}

export const ATLAS_SECTIONS: AtlasSection[] = [
  // ── Active ──────────────────────────────────────────────────────────────
  {
    id: "home",
    title: "Home",
    description: "The daily pulse — State of the D&D Multiverse.",
    icon: Home,
    route: "/",
    status: "active",
  },
  {
    id: "trends",
    title: "Trends",
    description: "Top classes, category heat, and emerging opportunities.",
    icon: TrendingUp,
    route: "/overview",
    status: "active",
  },
  {
    id: "articles",
    title: "Articles",
    description: "Daily dispatches from the Council of analysts.",
    icon: Newspaper,
    route: "/articles",
    status: "active",
  },
  {
    id: "bag",
    title: "Bag of Holding",
    description: "Cards and summaries you've stowed for later.",
    icon: Backpack,
    route: "/collection",
    status: "active",
  },
  // ── Planned (spec §3.6 sections not yet built) ──────────────────────────
  {
    id: "products",
    title: "Products & Opportunities",
    description: "Blue ocean, crowdfunding, and commercial angles.",
    icon: Gem,
    status: "planned",
    plannedNote: "Coming soon",
  },
  {
    id: "digital",
    title: "Digital & BG3",
    description: "Cross-medium footprint across BG3, VTTs, and SaaS.",
    icon: Gamepad2,
    status: "planned",
    plannedNote: "Coming soon",
  },
  {
    id: "deep-dives",
    title: "Deep Dives",
    description: "Single-stream Labs — one data source at a time.",
    icon: Microscope,
    status: "planned",
    plannedNote: "Coming soon",
  },
  {
    id: "methodology",
    title: "Methodology",
    description: "Confidence scoring, pipeline status, and caveats.",
    icon: BookOpen,
    status: "planned",
    plannedNote: "Coming soon",
  },
]
