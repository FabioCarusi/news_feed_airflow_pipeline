"""
Module for sending Telegram notifications.
"""
import time
import logging
import requests

logger = logging.getLogger(__name__)

def send_telegram_messages_in_chunks(
    bot_token: str, chat_id: str, messages: list[str]
):
    """
    Sends a list of messages to a Telegram chat in chunks using a synchronous approach.
    Adds a small delay between messages to avoid Telegram's flood limit.

    Args:
        bot_token (str): The API token of your Telegram bot.
        chat_id (str): The ID of the chat or channel to send the message to.
        messages (list[str]): A list of HTML strings, each string is a message.
    """
    if not messages:
        logger.info("No messages to send to Telegram.")
        return

    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    # Telegram generally allows about 20 messages per minute per chat.
    # A delay helps to avoid hitting flood limits.
    delay_between_messages = 0.5
    
    for i, message_text in enumerate(messages):
        try:
            logger.info("Sending message %d/%d to Telegram.", i + 1, len(messages))
            
            # Telegram has a limit of 4096 characters for HTML messages.
            if len(message_text) > 4096:
                logger.warning("Message too long, truncating...")
                message_text = message_text[:4090] + "..."

            payload = {
                "chat_id": chat_id,
                "text": message_text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }

            response = requests.post(api_url, data=payload, timeout=60)
            response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)

            logger.info("Message %d/%d sent successfully.", i + 1, len(messages))
            
            # Pause between messages to respect the API rate limit
            if i < len(messages) - 1:
                time.sleep(delay_between_messages)
        except requests.exceptions.RequestException as e:
            logger.error("Failed to send message %s/%s. Aborting. Error: %s", i+1, len(messages), e)
            raise

    logger.info("Finished sending all Telegram messages.")