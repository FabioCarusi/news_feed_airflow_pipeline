"""
Module for sending Telegram notifications.
"""

import asyncio
import logging

import nest_asyncio
import telegram

nest_asyncio.apply()

logger = logging.getLogger(__name__)


async def _send_single_telegram_message(
    telegram_client, chat_id: str, message_text: str
):
    """Async helper function to send a single message."""
    try:
        await telegram_client.send_message(
            chat_id=chat_id,
            text=message_text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        logger.info("Telegram message sent successfully to chat_id: %s", chat_id)
    except telegram.error.TelegramError as e:
        logger.error("Telegram error while sending message: %s", e)
        raise
    except Exception as e:
        logger.error("Generic error while sending Telegram message: %s", e)
        raise


def run_async(func):
    """
    Run decorated asynchronous function using asyncio.run.

    Args:
        func (Callable): The asynchronous function to be executed.

    Returns:
        Callable: A wrapper function that executes the async function synchronously.
    """

    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))

    return wrapper


@run_async
async def send_telegram_messages_in_chunks(
    bot_token: str, chat_id: str, messages: list[str]
):
    """
    Sends a list of HTML messages to a Telegram chat via the bot.
    Adds a small delay between messages to avoid Telegram's flood limit.

    Args:
        bot_token (str): The API token of your Telegram bot.
        chat_id (str): The ID of the chat or channel to send the message to.
        messages (list[str]): A list of HTML strings, each string is a message.
    """
    if not messages:
        logger.info("No messages to send to Telegram.")
        return

    telegram_client = telegram.Bot(token=bot_token)
    # Telegram generally allows about 20 messages per minute per chat.
    # 0.5 seconds delay means max 2 messages per second, which is safe.
    delay = 0.5
    for i, message_text in enumerate(messages):
        logger.info("Sending message %d/%d to Telegram.", i + 1, len(messages))
        # Telegram has a limit of 4096 characters for HTML messages.
        # Truncate if the message is too long.
        if len(message_text) > 4096:
            logger.warning("Telegram message content too long. It will be truncated.")
            message_text = message_text[:4090] + "..."
        await _send_single_telegram_message(telegram_client, chat_id, message_text)
        if i < len(messages) - 1:
            await asyncio.sleep(delay)
    logger.info("All Telegram messages sent.")
