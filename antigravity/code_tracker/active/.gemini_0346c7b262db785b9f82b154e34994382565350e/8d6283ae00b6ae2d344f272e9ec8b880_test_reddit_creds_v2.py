Žimport praw
import os

# Credentials from deploy_harvester.ps1
REDDIT_CLIENT_ID = "HGo2aTjVymaMre8kFMs9PA"
REDDIT_CLIENT_SECRET = "I-n58I_Ai3FWr08VK1TElErMLep6Sg"
REDDIT_USER_AGENT = "win:dnd-trends-tracker:v1.0 (by /u/Short_Let7420)"

def test_reddit():
    print("--- Testing Reddit Connectivity with Script Credentials ---")
    try:
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
        # Try to fetch hot posts from a small sub
        sub = reddit.subreddit("dndnext")
        for post in sub.hot(limit=1):
            print(f"SUCCESS! Connected to Reddit. Sample post: {post.title}")
            return True
    except Exception as e:
        print(f"FAILED to connect to Reddit: {e}")
        return False

if __name__ == "__main__":
    test_reddit()
Ž"(0346c7b262db785b9f82b154e34994382565350e26file:///C:/Users/Yorri/.gemini/test_reddit_creds_v2.py:file:///C:/Users/Yorri/.gemini