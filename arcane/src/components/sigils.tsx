/*
 * Trusight — per-report sigils for landing-page report cards.
 *
 * Lucide-style monochromatic line glyphs at a 24×24 viewBox. Both
 * stroke and any filled shapes use `currentColor`, so the parent's
 * Tailwind text-* class controls the rendered color. Card markup
 * pairs each sigil with its eyebrow tag on the same row above the
 * report title.
 */

type SigilProps = {
  className?: string
}

const BASE_PROPS = {
  viewBox: "0 0 24 24",
  fill: "none",
  stroke: "currentColor",
  strokeWidth: 1.75,
  strokeLinecap: "round" as const,
  strokeLinejoin: "round" as const,
} as const

export function SigilStackedCards({ className }: SigilProps) {
  return (
    <svg {...BASE_PROPS} className={className} aria-hidden>
      <rect x="4" y="4" width="11" height="14" rx="1" />
      <rect x="7" y="6" width="11" height="14" rx="1" />
      <rect x="10" y="8" width="10" height="13" rx="1" />
    </svg>
  )
}

export function SigilDecisionMatrix({ className }: SigilProps) {
  return (
    <svg {...BASE_PROPS} className={className} aria-hidden>
      <line x1="4" y1="12" x2="20" y2="12" />
      <line x1="12" y1="4" x2="12" y2="20" />
      <line x1="11" y1="4" x2="13" y2="4" />
      <line x1="11" y1="20" x2="13" y2="20" />
      <line x1="4" y1="11" x2="4" y2="13" />
      <line x1="20" y1="11" x2="20" y2="13" />
      <circle cx="16" cy="8" r="1.5" fill="currentColor" />
    </svg>
  )
}

export function SigilSentimentWaveform({ className }: SigilProps) {
  return (
    <svg {...BASE_PROPS} className={className} aria-hidden>
      <polyline points="3,15 7,9 10,13 14,5 17,15 21,10" />
    </svg>
  )
}

export function SigilThreeSilhouettes({ className }: SigilProps) {
  return (
    <svg {...BASE_PROPS} className={className} aria-hidden>
      <circle cx="5" cy="9" r="2" />
      <path d="M 2 18 A 3 3 0 0 1 8 18" />
      <circle cx="12" cy="9" r="2" />
      <path d="M 9 18 A 3 3 0 0 1 15 18" />
      <circle cx="19" cy="9" r="2" />
      <path d="M 16 18 A 3 3 0 0 1 22 18" />
    </svg>
  )
}

export function SigilEvidenceLadder({ className }: SigilProps) {
  return (
    <svg {...BASE_PROPS} className={className} aria-hidden>
      <rect x="4" y="15" width="4" height="5" />
      <rect x="10" y="10" width="4" height="10" />
      <rect x="16" y="5" width="4" height="15" />
    </svg>
  )
}

export function SigilPageWithStar({ className }: SigilProps) {
  return (
    <svg {...BASE_PROPS} className={className} aria-hidden>
      <rect x="6" y="4" width="12" height="16" rx="0.5" />
      <path d="M 12 8 L 13 10.5 L 15.5 10.8 L 13.7 12.5 L 14.2 15 L 12 13.8 L 9.8 15 L 10.3 12.5 L 8.5 10.8 L 11 10.5 Z" />
      <line x1="8" y1="17" x2="16" y2="17" />
    </svg>
  )
}

export function SigilHexHub({ className }: SigilProps) {
  return (
    <svg {...BASE_PROPS} className={className} aria-hidden>
      <path d="M 12 3 L 20 7.5 L 20 16.5 L 12 21 L 4 16.5 L 4 7.5 Z" />
      <line x1="12" y1="12" x2="12" y2="6" />
      <line x1="12" y1="12" x2="17" y2="14.5" />
      <line x1="12" y1="12" x2="7" y2="14.5" />
    </svg>
  )
}
