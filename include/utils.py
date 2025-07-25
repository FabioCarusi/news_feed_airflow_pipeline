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
    
    logger.info(f"Generazione contenuto email per {len(articles)} articoli.")

    if len(articles) == 0:
        logger.info("Nessun nuovo articolo da includere nell'email.")
        email_body = "Ciao, non ho trovato nuovi articoli interessanti oggi."
    else:
        email_body = f"""
        Il tuo Feed di Notizie Quotidiano - {datetime.now().strftime('%Y-%m-%d')}
        <span class="tg-spoiler>Ecco gli articoli più recenti e rilevanti per i tuoi interessi:</span>
        
        """

        for article in articles:
            title = article.get('title', 'Nessun titolo disponibile')
            url = article.get('url', '#')
            source = article.get('source', 'Sconosciuto')
            
            email_body += f"""
                        <b><a href="{url}" target="_blank">{title}</a></b><br>
                        <span class="tg-spoiler>Fonte: {source}</span>
            """

        email_body += f"""
                <span class="tg-spoiler>Questo è un feed di notizie automatico generato da Airflow.</span>
                <span class="tg-spoiler>Data di generazione: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</span>
                """
        logger.info("Contenuto email generato con successo.")
    return email_body