# Software Requirements Specification (SRS)
## Project: Arcane Analytics (dnd-trends-tracker)
**Version:** 1.0  
**Date:** 2025-12-31

---

### 1. Introduction
#### 1.1 Purpose
The purpose of this document is to define the functional and non-functional requirements for the **Arcane Analytics** platform (internal: `dnd-trends-tracker`). This system provides data-driven insights into the TTRPG ecosystem, specifically focusing on Dungeons & Dragons.

#### 1.2 Project Scope
The system is a cloud-native data pipeline and visualization suite that:
- Harvests data from social, commercial, and informational sources.
- Normalizes and scores trends using a multi-factor weighting engine.
- Generates AI-driven narrative insights using LLMs.
- Presents actionable data via a real-time web dashboard.

#### 1.3 Definitions and Acronyms
- **SRS**: Software Requirements Specification
- **BQ**: BigQuery (Google's Data Warehouse)
- **Hype Score**: A composite metric of interest (Wiki + Fandom + YouTube).
- **Play Score**: A metric of active usage (Roll20 + Reddit).
- **Buy Score**: A metric of commercial transaction intent (Kickstarter + BackerKit).

---

### 2. Overall Description
#### 2.1 Product Perspective
Arcane Analytics acts as a middle-tier intelligence layer between raw TTRPG community data and end-users (Content Creators, Publishers, Researchers). It is hosted entirely on **Google Cloud Platform (GCP)**.

#### 2.2 Functional Components
1. **Harvester Engine**: Collection scripts for Reddit, Wikipedia, YouTube, Fandom, and Commercial APIs.
2. **Normalization Layer (Silver)**: Standardizes disparate data using Percentile Ranking (`PERCENT_RANK`).
3. **Scoring Engine (Gold)**: Aggregates metrics into a unified "Trend Score".
4. **AI Newsroom**: Automated persona-based article generation (Daily Journalist).
5. **Bouncer API**: A secure interface serving JSON data to the frontend.
6. **Dashboard**: A premium, glassmorphic UI for data visualization.

---

### 3. External Interface Requirements
#### 3.1 User Interfaces
- **Web Dashboard**: Responsive HTML5/Vanilla CSS interface.
- **Visuals**: Radar charts (ApexCharts), tickers, and "Rarity-coded" trend cards.

#### 3.2 Software Interfaces
- **Reddit API**: PRAW (Python) for subreddit scanning.
- **BigQuery API**: For data storage and SQL-based transformations.
- **Vertex AI (Gemini 1.5 Flash)**: For narrative generation and unstructured data parsing.
- **Google Cloud Functions / Cloud Run**: For serverless compute execution.

---

### 4. System Features
#### 4.1 Data Ingestion (Harvesters)
- **ID-SF-1**: The system shall fetch daily metrics from 16+ whitelisted subreddits.
- **ID-SF-2**: The system shall track Google Trends "Pilot" data for 18,000+ keywords.
- **ID-SF-3**: The system shall extract "View Velocity" from YouTube videos tagged with matched keywords.

#### 4.2 Data Transformation
- **TX-SF-1**: The system shall normalize all sources into a 0.0 - 1.0 range daily.
- **TX-SF-2**: The system shall store raw, silver (normalized), and gold (scored) data in separate BigQuery datasets.

#### 4.3 AI & Insights
- **AI-SF-1**: The system shall generate daily reports in three personas: Tavern Keeper, Sage, and Goblin.
- **AI-SF-2**: The system shall parse ICv2 ranking PDFs/articles to extract commercial physical sales data.

---

### 5. Non-Functional Requirements
#### 5.1 Reliability
- The data pipeline should handle API rate limits gracefully with backoff strategies.
- Critical data streams (Trends, Wiki) must maintain >= 95% "Freshness" (updated within 72 hours).

#### 5.2 Security
- API Credentials (Reddit, GCP) shall be stored as Environment Variables or Secret Manager entries.
- The Bouncer API must implement CORS to restrict access only to approved domains (e.g., GitHub Pages).

#### 5.3 Performance
- Dashboard load time should be under 2 seconds for the initial view.
- BigQuery views must be optimized to return results in under 5 seconds.

---

### 6. Constraints
- **Regionality**: Compute is primarily restricted to `us-central1` for AI and `us-west1` for Trends data storage.
- **Cost**: The system should leverage serverless/pay-as-you-go GCP tiers to minimize idle costs.
