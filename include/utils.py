import json
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def load_news_sources_config(source_config_file: list[dict]):
    """Carica la configurazione delle fonti di notizie dal file JSON."""
    if not os.path.exists(source_config_file):
        raise FileNotFoundError(
            f"Config file not found: {source_config_file}"
        )

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

def generate_news_email_content(articles: list) -> str:
    """
    Genera il contenuto HTML dell'email con le notizie filtrate.

    Args:
        articles (list): Una lista di dizionari, dove ogni dizionario rappresenta un articolo.
                         Questi sono gli 'newly_added_articles' restituiti da store_filtered_news.

    Returns:
        str: Il contenuto HTML completo dell'email.
    """
    if not articles:
        logger.info("Nessun nuovo articolo da includere nell'email.")
        return "<p>Nessun nuovo articolo interessante trovato oggi.</p>"

    email_body = f"""
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

    for article in articles:
        title = article.get('title', 'Nessun titolo disponibile')
        url = article.get('url', '#')
        source = article.get('source', 'Sconosciuto')
        
        email_body += f"""
                <li>
                    <strong><a href="{url}" target="_blank">{title}</a></strong><br>
                    <span class="source">Fonte: {source}</span>
                </li>
        """

    email_body += f"""
            </ul>
            <div class="footer">
                <p>Questo è un feed di notizie automatico generato da Airflow.</p>
                <p>Data di generazione: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        </div>
    </body>
    </html>
    """
    logger.info("Contenuto email generato con successo.")
    return email_body