import os
import subprocess

# Reddit Credentials provided by User
REDDIT_CLIENT_ID = "dnd-trends-tracker"
REDDIT_CLIENT_SECRET = "HGo2aTjVymaMre8kFMs9PA"
REDDIT_USER_AGENT = "personal use script:Developers: Short_Let7420"

def test_reddit():
    print("--- Testing Reddit Connectivity ---")
    os.environ['REDDIT_CLIENT_ID'] = REDDIT_CLIENT_ID
    os.environ['REDDIT_CLIENT_SECRET'] = REDDIT_CLIENT_SECRET
    os.environ['REDDIT_USER_AGENT'] = REDDIT_USER_AGENT
    
    # Run the harvester in test mode or just check credentials via python
    import praw
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
