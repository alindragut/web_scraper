import asyncio
import aiohttp
from typing import Optional
import logging
from src.utils import config

# Get a logger instance for this specific module.
log = logging.getLogger(__name__)

class WebFetcher:
    DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

    def __init__(self, user_agent: Optional[str] = None):
        self.user_agent = user_agent or self.DEFAULT_USER_AGENT
        self.request_timeout = config.URL_FETCH_TIMEOUT_SECONDS
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            headers = {'User-Agent': self.user_agent}
            timeout = aiohttp.ClientTimeout(total=self.request_timeout)
            self._session = aiohttp.ClientSession(headers=headers, timeout=timeout)
        return self._session

    async def close_session(self):
        if self._session and not self._session.closed:
            await self._session.close()
            log.info("AIOHTTP session closed.")

    async def fetch_single_page_async(self, url: str) -> Optional[bytes]:
        """Fetches a single URL, returns html content as bytes or None on error."""
        session = await self._get_session()
        try:
            async with session.get(url, allow_redirects=True) as response:
                response.raise_for_status()
                content_type = response.headers.get('Content-Type', '').lower()
                if 'text/html' not in content_type:
                    log.warning(f"Skipping non-HTML content at {url} (Content-Type: {content_type})")
                    return None
                return await response.read()
        except aiohttp.ClientError as e:
            log.error(f"Fetch ClientError for {url}: {type(e).__name__}")
            return None
        except asyncio.TimeoutError:
            log.error(f"Fetch Timeout for {url}")
            return None
        except Exception as e:
            log.error(f"Unexpected fetch error for {url}", exc_info=True)
            return None
