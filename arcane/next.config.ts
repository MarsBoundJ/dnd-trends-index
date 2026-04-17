import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // next/image blocks remote hosts by default. Allow the Google avatar
  // CDN used by Auth.js sessions (Step 11). If we add other trusted
  // remote hosts (Firebase Storage, etc.) they go here too.
  images: {
    remotePatterns: [
      {
        protocol: "https",
        hostname: "lh3.googleusercontent.com",
      },
    ],
  },
};

export default nextConfig;
