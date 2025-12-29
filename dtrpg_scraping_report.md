# DTRPG/DMsGuild Scraping: Technical Frictions Report

## **Objective**
To extract the "Bestseller Medal" status (Copper, Silver, Gold, Platinum, etc.) and Product Metadata from DriveThruRPG's `metal.php` page to correlate sales velocity with search trends.

## **Approach & Tooling**
*   **Engine:** Python `playwright` (Async API) using Chromium logic.
*   **Target URL:** `https://www.drivethrurpg.com/metal.php` (Legacy layout listing products by medal tier).

## **Execution Log (Attempts & Failures)**

### **Attempt 1: Standard Headless Mode**
*   **Method:** Launched Chromium with `headless=True`.
*   **Result:** Immediate interception by Cloudflare Turnstile ("Verify you are human").
*   **Diagnosis:** Standard anti-bot behavior blocking non-interactive sessions.

### **Attempt 2: Headed Mode (Interactive)**
*   **Method:** Launched with `headless=False` to allow manual user verification of the Turnstile challenge.
*   **Result:**
    1.  Browser launched.
    2.  User manually clicked the verify box.
    3.  **Fatal Error:** The browser tab crashed immediately ("Sad Computer Face") or entered a reload loop.
*   **Diagnosis:** DTRPG's security rules likely detect the underlying WebDriver (Playwright/Puppeteer) hooks even in headed mode and terminate the session or connection.

### **Attempt 3: Stealth Configuration**
*   **Method:** Applied anti-detect arguments:
    *   `--disable-blink-features=AutomationControlled` (Hides `navigator.webdriver` flag).
    *   `--no-sandbox`
    *   Spoofed User-Agent to a standard Windows 10/Chrome 120 client.
    *   Extended timeouts to 90s.
*   **Result:**
    *   The browser remained open longer.
    *   User passed the CAPTCHA.
    *   **Data Block:** The target content (`table.standardText`) never loaded. The connection remained in a verified but "empty" state, or silently failed to retrieve the DOM.
    *   Debug HTML dumps showed either 0 bytes or the generic "Access Denied" template.

## **Conclusion**
DriveThruRPG is utilizing an aggressive configuration of Cloudflare Bot Management that fingerprint's the browser's TLS values (JA3 fingerprinting) or internal JavaScript execution environment. Simple WebDriver obfuscation (Playwright "Stealth") is insufficient.

## **Proposed Alternatives (for AI Consideration)**
1.  **Undetected-Chromedriver:** Use a patched Selenium driver specifically built to bypass Cloudflare.
2.  **Requests-HTML / Curl-Impersonate:** Abandon full browser execution and use a library that mimics the TLS fingerprint of a real browser (e.g., `curl-impersonate` or python wrappers like `curl_cffi`).
3.  **API Reverse Engineering:** Inspect network traffic for a mobile app API or internal JSON endpoint that might have laxer security than the frontend.
