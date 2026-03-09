import logging
import sys
import os

# Ensure local imports work
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from youtube_listener import run_listener
    from youtube_keyword_matcher import run_matcher
    from youtube_stats_updater import run_updater
    from youtube_sentiment_engine import run_sentiment_engine
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("YouTubeOrchestrator")

def run_orchestration():
    logger.info("🎬 Starting YouTube Social Intelligence Orchestration...")
    
    # 1. Ingest New Videos
    logger.info("Step 1/4: Running YouTube Listener (Ingestion)...")
    try:
        run_listener()
    except Exception as e:
        logger.error(f"Listener Failed: {e}")
        # We continue to enrich existing videos even if ingestion fails
    
    # 2. Match Keywords (Categorization)
    logger.info("Step 2/4: Running Keyword Matcher (Categorization)...")
    try:
        run_matcher()
    except Exception as e:
        logger.error(f"Matcher Failed: {e}")

    # 3. Update Statistics (Views/Velocity)
    logger.info("Step 3/4: Running Stats Updater...")
    try:
        run_updater()
    except Exception as e:
        logger.error(f"Stats Updater Failed: {e}")

    # 4. Analyze Sentiment (Comments)
    logger.info("Step 4/4: Running Sentiment Engine...")
    try:
        run_sentiment_engine()
    except Exception as e:
        logger.error(f"Sentiment Engine Failed: {e}")

    logger.info("✅ YouTube Orchestration Complete.")

if __name__ == "__main__":
    run_orchestration()
