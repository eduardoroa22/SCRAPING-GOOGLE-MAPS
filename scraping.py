import re
import os
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import requests
import time
from typing import List


EMAIL_MAX_PAGES_PER_SITE = int(os.getenv("EMAIL_MAX_PAGES_PER_SITE", "3"))
EMAIL_CRAWL_PACE_SECONDS = float(os.getenv("EMAIL_CRAWL_PACE_SECONDS", "0.5"))
EMAIL_USER_AGENT = os.getenv("EMAIL_USER_AGENT", "Mozilla/5.0 (compatible; StudioBot/1.0)")


EMAIL_REGEX = re.compile(r'(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b')

def _extract_emails_from_html(html: str) -> List[str]:
    emails = set()
    soup = BeautifulSoup(html, "html.parser")

    # 1) mailto:
    for a in soup.select('a[href^="mailto:"]'):
        href = a.get("href", "")
        addr = href[7:].split("?", 1)[0].strip()
        if addr and EMAIL_REGEX.fullmatch(addr):
            emails.add(addr)

    # 2) texto plano
    text = soup.get_text(" ", strip=True)
    for m in EMAIL_REGEX.findall(text):
        emails.add(m)

    return sorted(emails)

def find_emails_on_site(website: str) -> List[str]:
    if not website:
        return []

    try:
        parsed = urlparse(website)
        if not parsed.scheme:
            website = "https://" + website
            parsed = urlparse(website)
    except Exception:
        return []

    base = f"{parsed.scheme}://{parsed.netloc}"
    candidates = [website]
    for path in ("/contact", "/contact-us", "/contactus", "/about", "/about-us", "/studio", "/info"):
        candidates.append(urljoin(base, path))

    seen = set()
    found = set()
    pages = 0

    headers = {"User-Agent": EMAIL_USER_AGENT}

    for url in candidates:
        if pages >= EMAIL_MAX_PAGES_PER_SITE:
            break
        if not url or url in seen:
            continue
        if urlparse(url).netloc != parsed.netloc:
            continue

        seen.add(url)
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200:
                continue
            ct = (r.headers.get("Content-Type") or "").lower()
            if "text/html" not in ct:
                continue
            pages += 1
            for e in _extract_emails_from_html(r.text):
                found.add(e)
            time.sleep(EMAIL_CRAWL_PACE_SECONDS)
        except Exception:
            continue

    return sorted(found)

