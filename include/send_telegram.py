import telegram
import asyncio
import logging
import time 

logger = logging.getLogger(__name__)

async def _send_single_telegram_message(telegram_client, chat_id: str, message_text: str):
    """Async helper function to send a single message."""
    try:
        await telegram_client.send_message(
            chat_id=chat_id,
            text=message_text,
            parse_mode='HTML',
            disable_web_page_preview=True
        )
        logger.info(f"Telegram message sent successfully to chat_id: {chat_id}")
    except telegram.error.TelegramError as e:
        logger.error(f"Telegram error while sending message: {e}")
        raise
    except Exception as e:
        logger.error(f"Generic error while sending Telegram message: {e}")
        raise


def send_telegram_messages_in_chunks(bot_token: str, chat_id: str, messages: list[str]):
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
    MESSAGE_SEND_DELAY_SECONDS = 0.5 

    for i, message_text in enumerate(messages):
        logger.info(f'Sending message {i+1}/{len(messages)} to Telegram.')
        # Telegram has a limit of 4096 characters for HTML messages.
        # Truncate if the message is too long.
        if len(message_text) > 4096:
            logger.warning("Telegram message content too long. It will be truncated.")
            message_text = message_text[:4090] + "..." # Leave space for the 3 dots

        asyncio.run(_send_single_telegram_message(telegram_client, chat_id, message_text))
        
        if i < len(messages) - 1: # Don't delay after the last message
            time.sleep(MESSAGE_SEND_DELAY_SECONDS)

    logger.info("All Telegram messages sent.")