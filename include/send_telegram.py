import telegram
import logging
import asyncio

logger = logging.getLogger(__name__)

def send_telegram_message(bot_token: str, chat_id: str, message: str):
    """
    Invia un messaggio HTML a una chat Telegram tramite il bot.

    Args:
        bot_token (str): Il token API del tuo bot Telegram.
        chat_id (str): L'ID della chat o del canale a cui inviare il messaggio.
        message (str): Il contenuto HTML del messaggio da inviare.
    """
    
    logger.info(f'Message: {message}')
    
    telegram_client = telegram.Bot(token=bot_token)
    try:
        asyncio.run(
            telegram_client.send_message(
                chat_id=chat_id, 
                text=message, 
                parse_mode='HTML',
                disable_web_page_preview=True  # Disabilita le anteprime URL
                )
        )
    except telegram.error.TelegramError as e:
        logger.error(f"Errore durante l'invio del messaggio Telegram: {e}")
        raise
    except Exception as e:
        logger.error(f"Errore generico durante l'invio del messaggio Telegram: {e}")
        raise
        
    #telegram_api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    #
    #payload = {
    #    "chat_id": chat_id,
    #    "text": message,
    #    "parse_mode": "HTML",  # Importante per interpretare il HTML
    #    "disable_web_page_preview": True # Opzionale: disabilita le anteprime URL
    #}
#
    #try:
    #    response = requests.post(telegram_api_url, data=payload)
    #    response.raise_for_status()  # Solleva un errore per risposte HTTP non riuscite
    #    logger.info(f"Messaggio Telegram inviato con successo a chat_id: {chat_id}")
    #except requests.exceptions.RequestException as e:
    #    logger.error(f"Errore durante l'invio del messaggio Telegram: {e}")
    #    raise # Rilancia l'eccezione per far fallire il task di Airflow