# include/fetch_data.py
import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime
import json
import os

logger = logging.getLogger(__name__)

def fetch_news_article(article_url: str, data_dir: str):
    """
    Scarica una notizia da un URL dato e salva i dati grezzi.
    Ritorna il percorso al file JSON e alcuni metadati.
    """
    execution_date_str = datetime.now().strftime('%Y%m%d_%H%M%S') # Usa ora attuale per unicità
    output_json_path = os.path.join(data_dir, f'raw_news_{execution_date_str}_{os.path.basename(article_url).replace("/", "_")[:20]}.json')

    try:
        logger.info(f"Attempting to fetch data from: {article_url}")
        response = requests.get(article_url, timeout=15)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        soup = BeautifulSoup(response.text, 'lxml')

        title_tag = soup.find('h1')
        title = title_tag.get_text(strip=True) if title_tag else "No Title Found"

        # General approach for article text - needs refinement per site
        # For a generic site, look for common article body tags like <article>, <div class="content"> etc.
        # This example tries a few common ones. Adjust as needed.
        article_body = soup.find('article') or soup.find('div', class_='story-body') or soup.find('div', id='bodyContent') or soup.find('div', class_='mw-parser-output')
        
        paragraphs = article_body.find_all('p') if article_body else []
        article_text = "\n".join([p.get_text(strip=True) for p in paragraphs])
        
        if not article_text:
            logger.warning(f"No main article text found for {article_url}. Consider refining CSS selectors.")
            # Fallback to just body text if specific elements not found
            article_text = soup.body.get_text(strip=True)[:500] + "..." if soup.body else ""


        news_data = {
            'url': article_url,
            'title': title,
            'raw_text': article_text,
            'fetch_timestamp': datetime.now().isoformat(),
            'source': article_url.split('/')[2] # Estrae il dominio come sorgente
        }

        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(news_data, f, ensure_ascii=False, indent=4)

        logger.info(f"Successfully fetched data from {article_url} and saved to {output_json_path}")
        return {
            'raw_news_file_path': output_json_path,
            'article_title': title,
            'article_url': article_url,
            'source': news_data['source'],
            'status': 'success'
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching {article_url}: {e}")
        return {
            'raw_news_file_path': None,
            'article_title': None,
            'article_url': article_url,
            'source': article_url.split('/')[2],
            'status': 'failed',
            'error_message': str(e)
        }
    except Exception as e:
        logger.error(f"An unexpected error occurred during scraping {article_url}: {e}", exc_info=True)
        return {
            'raw_news_file_path': None,
            'article_title': None,
            'article_url': article_url,
            'source': article_url.split('/')[2],
            'status': 'failed',
            'error_message': str(e)
        }