"""
Telegram Notification Module.

This module provides functionality to send notifications to Telegram
using the Telegram Bot API.
"""

import logging
import re
import time

from bs4 import BeautifulSoup

import requests

logger = logging.getLogger(__name__)

# Telegram API constants
TELEGRAM_MAX_MESSAGE_LENGTH = 4096
TELEGRAM_TRUNCATION_SUFFIX = "..."
TELEGRAM_TRUNCATION_LIMIT = TELEGRAM_MAX_MESSAGE_LENGTH - len(TELEGRAM_TRUNCATION_SUFFIX)
MESSAGE_DELAY_SECONDS = 0.5  # Delay between messages to avoid flood limits
REQUEST_TIMEOUT_SECONDS = 60

ALLOWED_TAGS = {
    "b", "strong", "i", "em", "u", "ins",
    "s", "strike", "del",
    "a", "code", "pre", "blockquote",
    "span",
}


def sanitize_telegram_html(text: str) -> str:
    soup = BeautifulSoup(text, "html.parser")

    for tag in soup.find_all():
        if tag.name not in ALLOWED_TAGS:
            tag.replace_with(tag.string or "")
            continue
        if tag.name == "a":
            allowed = {"href"}
            tag.attrs = {k: v for k, v in tag.attrs.items() if k in allowed}
        elif tag.name == "span":
            allowed = {"class"}
            tag.attrs = {k: v for k, v in tag.attrs.items() if k in allowed}
            if tag.get("class") != ["tg-spoiler"]:
                tag.replace_with(tag.string or "")
        else:
            tag.attrs = {}

    result = str(soup)
    result = re.sub(r"\n\s*\n", "\n", result)
    return result.strip()


def send_telegram_messages_in_chunks(
    bot_token: str, chat_id: str, messages: list[str]
) -> None:
    """Send a list of messages to a Telegram chat.

    Sends messages sequentially with a delay between each to respect
    Telegram's rate limits (approximately 20 messages per minute per chat).

    Args:
        bot_token: The API token of your Telegram bot.
        chat_id: The ID of the chat or channel to send messages to.
        messages: List of HTML-formatted message strings to send.

    Raises:
        requests.exceptions.RequestException: If a message fails to send.
    """
    if not messages:
        logger.info("No messages to send to Telegram")
        return

    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    for index, message_text in enumerate(messages, start=1):
        try:
            logger.info("Sending message %d/%d to Telegram", index, len(messages))

            message_text = sanitize_telegram_html(message_text)

            # Truncate message if it exceeds Telegram's limit
            if len(message_text) > TELEGRAM_MAX_MESSAGE_LENGTH:
                logger.warning(
                    "Message %d exceeds %d characters, truncating",
                    index,
                    TELEGRAM_MAX_MESSAGE_LENGTH
                )
                message_text = (
                    message_text[:TELEGRAM_TRUNCATION_LIMIT] + TELEGRAM_TRUNCATION_SUFFIX
                )

            payload = {
                "chat_id": chat_id,
                "text": message_text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }

            response = requests.post(
                api_url,
                data=payload,
                timeout=REQUEST_TIMEOUT_SECONDS
            )

            if response.status_code == 400:
                logger.warning(
                    "Message %d rejected as HTML, retrying as plain text",
                    index
                )
                payload.pop("parse_mode")
                response = requests.post(
                    api_url,
                    data=payload,
                    timeout=REQUEST_TIMEOUT_SECONDS
                )

            response.raise_for_status()

            logger.info("Message %d/%d sent successfully", index, len(messages))

            # Pause between messages to respect API rate limits
            if index < len(messages):
                time.sleep(MESSAGE_DELAY_SECONDS)

        except requests.exceptions.RequestException as e:
            logger.error(
                "Failed to send message %d/%d: %s response: %s",
                index,
                len(messages),
                response,
                e
            )
            raise

    logger.info("Successfully sent all %d Telegram messages", len(messages))
