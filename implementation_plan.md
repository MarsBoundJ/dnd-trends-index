# Google Trends Deep Dive: Implementation Plan

## Goal
Implement a Python analysis suite (`analyze_trends_deep_dive.py`) to extract advanced insights from BigQuery `dnd-trends-index` project, specifically focusing on Google Trends data joined with Categories.

## Proposed Changes

### [NEW] `analyze_trends_deep_dive.py`
A new script utilizing `google-cloud-bigquery` and `pandas` (if available, or raw aggregation) to perform:
1.  **Dynamic Category Leaderboards**: Query for Top items by `interest` score.
    *   **Logic**: If Category Count > 200, show **Top 40**. Else, show **Top 20**.
2.  **Exploratory Volatility Analysis**:
    *   Calculate Standard Deviation of `interest` over time per term.
    *   Classify terms/categories as "Steady", "Volatile", or "Spikey".
    *   Determine if trends are "Fads" (Quick Rise/Fall) or "Staples" (Steady).
3.  **Long-Term Momentum**: Calculate Month-over-Month and Quarter-over-Quarter growth, rather than just weekly.
4.  **Opportunity Matrix**: Identify terms with High Trend Interest but Low "Saturation" (Wiki/Fandom presence).
5.  **Seasonality Check**: Aggregate average interest by Day of Week to find "DM Prep Day".

### [NEW] `data_science_strategy.md`
A documentation artifact explaining the metrics and techniques used, serving as the "Data Scientist's Report" structure.

## Verification Plan

### Automated Tests
*   Run `python analyze_trends_deep_dive.py --test` (I will implement a --test flag to run a small limit query).
*   Verify output CSVs or text logs are generated.

### Manual Verification
*   Execute the script and inspect the output `trends_deep_dive_report.md`.
*   Check if "Vecna" (known trending) appears in Top Lists.
