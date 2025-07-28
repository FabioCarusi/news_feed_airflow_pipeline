import json
import os
import logging
import sqlite3
from webscraping_airflow_pipeline.include.store_news import get_db_path
from datetime import datetime

logger = logging.getLogger(__name__)


def load_news_sources_config(source_config_file: list[dict]):
    """Carica la configurazione delle fonti di notizie dal file JSON."""
    if not os.path.exists(source_config_file):
        raise FileNotFoundError(f"Config file not found: {source_config_file}")

    with open(source_config_file, "r", encoding="utf-8") as f:
        sources = json.load(f)
    logger.info(f"Loaded {len(sources)} news sources from config file.")
    return sources


def load_keywords_config(keyword_config_file: list[str]):
    """Carica la configurazione delle keyword dal file JSON."""
    if not os.path.exists(keyword_config_file):
        raise FileNotFoundError(f"Config file not found: {keyword_config_file}")

    with open(keyword_config_file, "r", encoding="utf-8") as f:
        keywords = json.load(f)
    logger.info(f"Loaded {len(keywords)} keywords from config file.")
    return keywords


def generate_news_email_content(articles: list, is_telegram: bool = False) -> str:
    """
    Genera il contenuto HTML dell'email/notifica con le notizie filtrate.

    Args:
        articles (list): Una lista di dizionari, dove ogni dizionario rappresenta un articolo.
        is_telegram (bool): Se True, ottimizza il formato per Telegram (più compatto e compatibile).

    Returns:
        str: Il contenuto HTML completo (o parte di esso).
    """
    if not articles:
        if is_telegram:
            return "Nessun nuovo articolo interessante trovato oggi."
        else:
            return "<p>Nessun nuovo articolo interessante trovato oggi.</p>"

    # Struttura base del corpo dell'email/notifica
    if not is_telegram:
        # Formato più verboso per email complete (standard HTML)
        html_template_start = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ width: 80%; margin: 20px auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px; background-color: #f9f9f9; }}
                h1 {{ color: #0056b3; }}
                h2 {{ color: #007bff; border-bottom: 1px solid #eee; padding-bottom: 5px; margin-top: 20px; }}
                ul {{ list-style-type: none; padding: 0; }}
                li {{ margin-bottom: 10px; }}
                a {{ color: #007bff; text-decoration: none; }}
                a:hover {{ text-decoration: underline; }}
                .source {{ font-size: 0.9em; color: #666; }}
                .footer {{ margin-top: 30px; font-size: 0.8em; color: #999; text-align: center; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Il tuo Feed di Notizie Quotidiano - {datetime.now().strftime('%Y-%m-%d')}</h1>
                <p>Ecco gli articoli più recenti e rilevanti per i tuoi interessi:</p>
                <ul>
        """
        html_template_end = f"""
                </ul>
                <div class="footer">
                    <p>Questo è un feed di notizie automatico generato da Airflow.</p>
                    <p>Data di generazione: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
            </div>
        </body>
        </html>
        """
    else:
        # Formato COMPATIBILE CON TELEGRAM: solo i tag supportati
        html_template_start = ""
        html_template_end = ""

    articles_html = ""
    for article in articles:
        title = article.get("title", "Nessun titolo disponibile")
        url = article.get("url", "#")
        source = article.get("source", "Sconosciuto")

        # Formattazione specifica per Telegram con i tag supportati
        if is_telegram:
            # Usiamo <b> per grassetto, <i> per corsivo (fonte), e <a href> per i link.
            articles_html += (
                f'• <b><a href="{url}">{title}</a></b>'
                f"  <i>Fonte: {source} </i>"
                f"\n"
            )
        else:
            # Formattazione originale per email (con ul, li, strong, span)
            articles_html += f"""
                <li>
                    <strong><a href="{url}" target="_blank">{title}</a></strong><br>
                    <span class="source">Fonte: {source} </span>
                </li>
            """

    if not is_telegram:
        return html_template_start + articles_html + html_template_end
    else:
        return articles_html


def generate_telegram_message_chunks(articles: list, chunk_size: int = 10) -> list[str]:
    """
    Genera una lista di stringhe HTML, dove ogni stringa contiene un blocco di articoli
    ottimizzato per Telegram.

    Args:
        articles (list): Lista di dizionari articolo.
        chunk_size (int): Numero massimo di articoli per ogni messaggio.

    Returns:
        list[str]: Una lista di stringhe, ogni stringa è un messaggio Telegram.
    """
    chunks = []

    # Intestazione per il primo messaggio, compatibile con Telegram HTML
    header = f"<b>Il tuo Feed di Notizie Quotidiano - {datetime.now().strftime('%Y-%m-%d')}</b>\n\n"
    header += "Ecco gli articoli più recenti e rilevanti per i tuoi interessi:\n\n"

    if not articles:
        return [header + generate_news_email_content([], is_telegram=True)]

    current_chunk_articles = []

    for i, article in enumerate(articles):
        current_chunk_articles.append(article)

        if (i + 1) % chunk_size == 0 or (i + 1) == len(articles):
            chunk_body = generate_news_email_content(
                current_chunk_articles, is_telegram=True
            )

            if not chunks:
                chunks.append(header + chunk_body)
            else:
                chunks.append(chunk_body)

            current_chunk_articles = []

    if not chunks:
        return [header + generate_news_email_content([], is_telegram=True)]

    return chunks

