import requests
import os
import logging
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

class PlayNowAPIClient:
    BASE_URL = "https://content.sb.playnow.com/content-service/api/v1/q"
    
    def __init__(self, cookie=None):
        self.session = requests.Session()
        self.cookie = cookie or os.environ.get("PLAYNOW_COOKIE")
        
        self.headers = {
            "accept": "application/json",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        if self.cookie:
            # We don't log the full cookie for security
            logger.info("Using PlayNow cookie from environment/param.")
            self.headers["Cookie"] = self.cookie
        else:
            logger.info("No PlayNow cookie provided; proceeding without it.")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def fetch_event_list(self, drilldown_tag_ids="220", include_child_markets=True, event_sorts="MTCH,TNMT"):
        """
        Fetches the list of events for a given competition (NHL is 220).
        Returns (request_url, response_json).
        """
        params = {
            "eventSortsIncluded": event_sorts,
            "includeChildMarkets": str(include_child_markets).lower(),
            "drilldownTagIds": drilldown_tag_ids,
            "lang": "en-US",
            "channel": "I"
        }
        url = f"{self.BASE_URL}/event-list"
        logger.info(f"Fetching event list from {url} with params {params}")
        response = self.session.get(url, headers=self.headers, params=params, timeout=15)
        response.raise_for_status()
        return response.url, response.json()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def fetch_event_details(self, event_ids, include_child_markets=True):
        """
        Fetches detailed info for specific event IDs.
        Returns (request_url, response_json).
        """
        if isinstance(event_ids, list):
            event_ids = ",".join(map(str, event_ids))
            
        params = {
            "eventIds": event_ids,
            "includeChildMarkets": str(include_child_markets).lower(),
            "lang": "en-US",
            "channel": "I"
        }
        url = f"{self.BASE_URL}/events-by-ids"
        logger.info(f"Fetching event details from {url} (eventIds: {event_ids})")
        response = self.session.get(url, headers=self.headers, params=params, timeout=15)
        response.raise_for_status()
        return response.url, response.json()
