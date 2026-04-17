import type { Metadata } from "next";
import { Spectral, Inter, JetBrains_Mono } from "next/font/google";
import "./globals.css";
import { SageProvider } from "@/components/sage-panel";
import { ConceptDrawerProvider } from "@/components/concept-drawer";
import { AtlasProvider } from "@/components/atlas";
import { SiteHeader } from "@/components/site-header";

/*
 * Font loading per FRONTEND_DESIGN_SPEC.md §4.2.
 * Three fonts, each doing exactly one job:
 *   - Spectral     → headers & concept names (warm, WotC book feel)
 *   - Inter        → body & UI labels (Stripe / Linear / Figma default)
 *   - JetBrains Mono → numbers & tabular data (terminal-premium)
 *
 * Each font is exposed as a CSS variable on <html>, which globals.css
 * aliases into Tailwind's --font-sans / --font-display / --font-mono
 * theme tokens. Font choice is fixed across all personas — persona
 * differentiation uses color, not fonts.
 */

const spectral = Spectral({
  variable: "--font-spectral",
  subsets: ["latin"],
  weight: ["400", "500", "600", "700"],
  display: "swap",
});

const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin"],
  display: "swap",
});

const jetbrainsMono = JetBrains_Mono({
  variable: "--font-jetbrains-mono",
  subsets: ["latin"],
  display: "swap",
});

export const metadata: Metadata = {
  title: "Arcane Analytics",
  description:
    "A Fantasy Bloomberg Terminal for the D&D multiverse — market intelligence for Hasbro, WotC, and the people who love the game.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="en"
      className={`${spectral.variable} ${inter.variable} ${jetbrainsMono.variable}`}
    >
      <body className="min-h-screen bg-obsidian text-parchment antialiased">
        {/*
         * SageProvider is the root-level chat context so any CardChrome on
         * any page can pop the Sage panel open via its `sageContext` prop.
         * It's a "use client" boundary — children below can still be RSCs.
         */}
        <SageProvider>
          <ConceptDrawerProvider>
            <AtlasProvider>
              <SiteHeader />
              {children}
            </AtlasProvider>
          </ConceptDrawerProvider>
        </SageProvider>
      </body>
    </html>
  );
}
