"""
OpenRouter Client
=================
Client for accessing Claude via OpenRouter API.
"""

import logging
import requests
from config import Config

logger = logging.getLogger(__name__)


class OpenRouterClient:
    """Client for Claude via OpenRouter"""
    
    def __init__(self):
        self.api_key = Config.OPENROUTER_API_KEY
        self.base_url = Config.OPENROUTER_BASE_URL
        self.model = Config.CLAUDE_MODEL
    
    def chat(self, messages: list, system_prompt: str = None) -> str:
        """
        Send a chat request to Claude.
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            system_prompt: Optional system prompt to prepend
            
        Returns:
            Claude's response as a string
        """
        if not self.api_key:
            logger.error("OpenRouter API key not configured")
            return "⚠️ OpenRouter API key not configured. Please set OPENROUTER_API_KEY environment variable."
        
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
        
        logger.info(f"OpenRouter request: model={self.model}, messages={len(all_messages)}")
        
        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=payload,
                timeout=60
            )
            response.raise_for_status()
            data = response.json()
            
            result = data["choices"][0]["message"]["content"]
            logger.info(f"OpenRouter response: {len(result)} chars")
            return result
            
        except requests.exceptions.Timeout:
            logger.error("OpenRouter timeout")
            return "❌ Request timed out. Please try again."
        except requests.exceptions.RequestException as e:
            logger.error(f"OpenRouter error: {str(e)}")
            return f"❌ Error communicating with AI: {str(e)}"
        except (KeyError, IndexError) as e:
            logger.error(f"OpenRouter parse error: {str(e)}")
            return f"❌ Unexpected response format: {str(e)}"
