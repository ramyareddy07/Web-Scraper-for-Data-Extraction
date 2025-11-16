import logging
import re
import time
from datetime import datetime
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse, urljoin

class WebScraper:
    def __init__(self, config, csv_store, mysql_store=None, max_retries=3, timeout=10):
        self.config = config
        self.csv_store = csv_store
        self.mysql_store = mysql_store
        self.session = self._build_session(max_retries, timeout)
        self.seen_urls = set()

    def _build_session(self, max_retries, timeout):
        s = requests.Session()
        retries = Retry(total=max_retries, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504))
        adapter = HTTPAdapter(max_retries=retries)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        s.headers.update({"User-Agent": self.config.user_agent})
        s.request_timeout = timeout
        return s

    def fetch(self, url):
        try:
            resp = self.session.get(url, timeout=getattr(self.session, 'request_timeout', 10))
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logging.error("Fetch failed for %s: %s", url, e)
            return None

    def parse_items(self, html, base_url):
        soup = BeautifulSoup(html, "html.parser")
        items = soup.select(self.config.item_selector)
        results = []
        for el in items:
            record = {}
            record['source_url'] = base_url
            for field_name, selector in self.config.fields.items():
                try:
                    if selector.startswith("attr:"):
                        parts = selector.split(":", 1)[1]
                        sel, attr = parts.split("|", 1) if "|" in parts else (parts, None)
                        found = el.select_one(sel)
                        val = found.get(attr).strip() if found and attr and found.get(attr) else (found.get_text(" ", strip=True) if found else "")
                    else:
                        found = el.select_one(selector)
                        val = found.get_text(" ", strip=True) if found else ""
                except Exception:
                    val = ""
                record[field_name] = self._clean_text(val)
            record['scraped_at'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            record['extra'] = {}
            if self._validate(record):
                results.append(record)
        return results

    def _clean_text(self, text):
        if text is None:
            return ""
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _validate(self, record):
        if not record.get('title'):
            return False
        return True

    def find_next(self, html, current_url):
        if not self.config.next_selector:
            return None
        soup = BeautifulSoup(html, "html.parser")
        nxt = soup.select_one(self.config.next_selector)
        if not nxt:
            return None
        href = nxt.get('href') or nxt.get('data-href') or ""
        if not href:
            return None
        if href.startswith('http'):
            return href
        if href.startswith('/'):
            base = f"{urlparse(current_url).scheme}://{urlparse(current_url).netloc}"
            return urljoin(base, href)
        return urljoin(current_url, href)

    def save_records(self, records: List[Dict]):
        for r in records:
            try:
                self.csv_store.write(r)
            except Exception as e:
                logging.error("CSV write error: %s", e)
            if self.mysql_store:
                try:
                    self.mysql_store.upsert(r)
                except Exception as e:
                    logging.error("MySQL write error: %s", e)

    def run(self):
        url = self.config.start_url
        pages = 0
        while url and (self.config.max_pages <= 0 or pages < self.config.max_pages):
            if url in self.seen_urls:
                break
            logging.info("Scraping page: %s", url)
            html = self.fetch(url)
            if not html:
                break
            items = self.parse_items(html, url)
            logging.info("Found %d items on page", len(items))
            self.save_records(items)
            self.seen_urls.add(url)
            pages += 1
            next_url = self.find_next(html, url)
            if not next_url:
                break
            url = next_url
            time.sleep(self.config.delay)