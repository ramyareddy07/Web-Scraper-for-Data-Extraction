
from typing import Optional, Dict

class ScraperConfig:
    def __init__(self, start_url: str, item_selector: str, fields: Dict[str, str], next_selector: Optional[str], max_pages: int, delay: float, user_agent: str):
        self.start_url = start_url
        self.item_selector = item_selector
        self.fields = fields
        self.next_selector = next_selector
        self.max_pages = max_pages
        self.delay = delay
        self.user_agent = user_agent




