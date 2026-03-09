import requests
from bs4 import BeautifulSoup

BASE = "https://dnd5e.fandom.com"

def check_article_categories(slug):
    url = f"{BASE}/wiki/{slug}"
    print(f"Checking categories for {url}...")
    try:
        r = requests.get(url, timeout=10)
        soup = BeautifulSoup(r.content, 'html.parser')
        # Categories are usually in div.page-header__categories or div.categories or li.category-normal
        # Fandom often puts them in a special 'Categories' section at bottom.
        # Let's verify commonly used classes.
        
        cats = []
        links = soup.find_all("a", href=True)
        for link in links:
            if "/wiki/Category:" in link['href']:
                cats.append(link.get_text())
                
        # Filter duplicates and 'Add category'
        cats = list(set([c for c in cats if c and "Add category" not in c]))
        print(f"Categories found: {cats}")
    except Exception as e:
        print(e)

check_article_categories("Fireball")
check_article_categories("Adult_Red_Dragon")
check_article_categories("Vorpal_Sword")
check_article_categories("Wizard")
check_article_categories("Human")
check_article_categories("Neverwinter")
