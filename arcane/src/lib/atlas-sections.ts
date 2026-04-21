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

import type { ComponentType } from "react"
import {
  Home,
  TrendingUp,
  Newspaper,
  Gem,
  Gamepad2,
  Microscope,
  BookOpen,
  KeyRound,
  Crown,
} from "lucide-react"
import { BagOfHoldingSigil } from "@/components/bag-of-holding-sigil"

export type AtlasSectionStatus = "active" | "planned"

export interface AtlasSection {
  id: string
  title: string
  /** One-line Sage-voiced blurb — analytical, no marketing-speak. */
  description: string
  /**
   * Icon component rendered inside the tile. Accepts a `className` prop.
   * Lucide icons satisfy this shape; composite sigils (e.g. the Bag of
   * Holding's PackageOpen + Infinity charm) do too.
   */
  icon: ComponentType<{ className?: string }>
  /** Next.js route. Omit for planned sections. */
  route?: string
  status: AtlasSectionStatus
  /** Optional note shown under planned sections (e.g. "Post-launch"). */
  plannedNote?: string
  /**
   * Admin-only tile — hidden entirely unless the signed-in user's email is
   * in NEXT_PUBLIC_ADMIN_EMAILS. The SERVER-side proxy.ts enforces the
   * actual gate at /admin/*; this flag is purely for discoverability, so
   * non-admins never see a tile they can't use.
   */
  adminOnly?: boolean
}

/**
 * Comma-separated mirror of the server-only ADMIN_EMAILS. Keep in sync —
 * the Atlas filter here and the proxy allowlist are two sides of the
 * same check.
 */
const ADMIN_EMAILS = new Set(
  (process.env.NEXT_PUBLIC_ADMIN_EMAILS ?? "")
    .split(",")
    .map((e) => e.trim().toLowerCase())
    .filter(Boolean),
)

/**
 * Returns the Atlas sections visible to the given session. Non-admins
 * get every non-adminOnly section; admins additionally get adminOnly
 * tiles.
 */
export function visibleAtlasSections(
  userEmail: string | null | undefined,
): AtlasSection[] {
  const isAdmin =
    typeof userEmail === "string" && ADMIN_EMAILS.has(userEmail.toLowerCase())
  return ATLAS_SECTIONS.filter((s) => !s.adminOnly || isAdmin)
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
    icon: BagOfHoldingSigil,
    route: "/collection",
    status: "active",
  },
  {
    id: "admin",
    title: "Admin",
    description: "Back-office — harvests, data-quality monitors, run logs.",
    icon: KeyRound,
    route: "/admin",
    status: "active",
    adminOnly: true,
  },
  // Step 9.9 — flagship commercial feature.
  {
    id: "universes-beyond",
    title: "Universes Beyond",
    description:
      "Non-D&D/non-MTG IPs scored by license-fit for MTG crossover or D&D-setting adaptation.",
    icon: Crown,
    route: "/matrix/universes-beyond",
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
