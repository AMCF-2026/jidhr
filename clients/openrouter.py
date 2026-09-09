"""
OpenRouter Client
=================
Client for accessing Claude via OpenRouter API.
"""

import logging
import time

import requests
from config import Config

logger = logging.getLogger(__name__)

# One retry, after this pause, when OpenRouter rate-limits us.
RATE_LIMIT_RETRY_SECONDS = 2


class OpenRouterError(RuntimeError):
    """A chat completion could not be obtained.

    Raised rather than returned as text. The old code handed back a string
    starting "❌ Error communicating with AI", which every caller then stored
    and displayed as if it were Claude's answer — a failed call was
    indistinguishable from a successful one, and the error text was appended
    to conversation history as a real assistant turn.
    """

    def __init__(self, status, message):
        self.status = status
        self.message = message
        super().__init__(f"OpenRouter failed (status={status}): {message}")


class OpenRouterClient:
    """Client for Claude via OpenRouter"""
    
    def __init__(self):
        self.api_key = Config.OPENROUTER_API_KEY
        self.base_url = Config.OPENROUTER_BASE_URL
        self.model = Config.CLAUDE_MODEL
    
    def chat(self, messages: list, system_prompt: str = None, temperature: float = None) -> str:
        """
        Send a chat request to Claude.

        Args:
            messages: List of message dicts with 'role' and 'content'
            system_prompt: Optional system prompt to prepend
            temperature: Optional sampling temperature. If None, omitted
                from the request so OpenRouter's default applies.

        Returns:
            Claude's response as a string

        Raises:
            OpenRouterError: on any HTTP, network or parse failure. Callers
                must decide what the user sees; this client never returns an
                error message dressed up as an answer.
        """
        if not self.api_key:
            logger.error("OpenRouter API key not configured")
            raise OpenRouterError(None, "OPENROUTER_API_KEY is not configured")
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://amuslimcf.org",
            "X-Title": "Jidhr - AMCF Operations Assistant"
        }
        
        # Build message list
        all_messages = []
        if system_prompt:
            all_messages.append({"role": "system", "content": system_prompt})
        all_messages.extend(messages)
        
        payload = {
            "model": self.model,
            "messages": all_messages
        }
        if temperature is not None:
            payload["temperature"] = temperature
        
        logger.info(f"OpenRouter request: model={self.model}, messages={len(all_messages)}")

        try:
            return self._attempt(headers, payload)
        except OpenRouterError as first:
            if first.status != 429:
                raise
            # Rate limited: one retry, then give up. Retrying anything else
            # would just double the wait before the user hears about it.
            logger.warning(
                "OpenRouter rate limited; retrying once in %ss",
                RATE_LIMIT_RETRY_SECONDS)
            time.sleep(RATE_LIMIT_RETRY_SECONDS)
            return self._attempt(headers, payload)

    def _attempt(self, headers: dict, payload: dict) -> str:
        """One request. Raises OpenRouterError on any failure."""
        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=payload,
                timeout=60,
            )
        except requests.exceptions.Timeout as e:
            logger.error("OpenRouter timeout")
            raise OpenRouterError("timeout", "the request timed out") from e
        except requests.exceptions.RequestException as e:
            logger.error(f"OpenRouter transport error: {e}")
            raise OpenRouterError(None, str(e)) from e

        status = response.status_code
        if status >= 400:
            body = (response.text or "")[:200]
            logger.error(f"OpenRouter HTTP {status}: {body}")
            raise OpenRouterError(status, body or f"HTTP {status}")

        try:
            data = response.json()
            result = data["choices"][0]["message"]["content"]
        except (ValueError, KeyError, IndexError, TypeError) as e:
            logger.error(f"OpenRouter parse error: {e}")
            raise OpenRouterError(status, f"unexpected response format: {e}") from e

        logger.info(f"OpenRouter response: {len(result)} chars")
        return result
