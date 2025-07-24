# include/fetch_news.py
import requests
from bs4 import BeautifulSoup
import logging
from urllib.parse import urljoin  # Per gestire URL relativi

dag_logger = logging.getLogger(__name__)


def fetch_headlines_from_source(
    url: str, title_selector: str, link_selector: str
) -> list[dict]:
    """
    Scarica titoli e link da una pagina web data la URL e i selettori CSS.
    """
    articles_data = []
    try:
        dag_logger.info(f"Fetching data from URL: {url}")

        # Effettua la richiesta HTTP
        response = requests.get(url, timeout=15)
        response.raise_for_status()  # Lancia un errore per status codes HTTP 4xx/5xx (es. 404, 500)

        # Parsifica il contenuto HTML con BeautifulSoup e lxml (assicurati lxml sia installato!)
        soup = BeautifulSoup(response.text, "lxml")

        # Trova tutti gli elementi che contengono i titoli usando il selettore fornito
        title_elements = soup.select(title_selector)

        if not title_elements:
            dag_logger.warning(
                f"No elements found with title_selector '{title_selector}' for URL: {url}"
            )
            return []

        for title_tag in title_elements:
            title = title_tag.get_text(strip=True)  # Estrai il testo pulito del titolo
            link = ""

            # Cerca il link associato al titolo. La logica può variare:
            # 1. Se il titolo stesso è un link (es. <a href="...">Titolo</a>)
            if title_tag.name == "a" and title_tag.has_attr("href"):
                link = title_tag["href"]
            else:
                # 2. Cerca un link (<a>) all'interno del tag del titolo (es. <h2><a>Titolo</a></h2>)
                link_element_in_title = title_tag.find("a")
                if link_element_in_title and link_element_in_title.has_attr("href"):
                    link = link_element_in_title["href"]
                elif link_selector:
                    # 3. Se non trovato nei casi precedenti, prova a usare il link_selector fornito.
                    # Questo è il caso più delicato e dipendente dalla struttura HTML.
                    # Idealmente, link_selector dovrebbe puntare direttamente all'elemento <a> desiderato.
                    # Qui facciamo un'ipotesi più generica, ma potrebbe necessitare di tuning.

                    # Prova a trovare link correlati all'elemento del titolo
                    # Potremmo dover risalire all'elemento padre comune
                    # Per una soluzione più robusta qui, dovresti ispezionare l'HTML
                    # e trovare il selettore che cattura il div/elemento che contiene sia il titolo che il link.

                    # Un'euristica semplice: cerca tutti gli elementi con link_selector e
                    # vedi se il titolo che abbiamo estratto è nel loro testo.
                    potential_link_elements = soup.select(link_selector)
                    for le in potential_link_elements:
                        if le.get_text(strip=True) == title and le.has_attr("href"):
                            link = le["href"]
                            break
                        # A volte il link non ha esattamente il titolo, ma è in un elemento vicino
                        # potremmo cercare un link nel genitore comune del title_tag.
                        # Per ora, manteniamo una logica più diretta.

            # Normalizza il link: se è relativo (es. /blog/post-1), trasformalo in assoluto
            if link and not link.startswith(("http://", "https://")):
                link = urljoin(url, link)

            if title and link:
                articles_data.append({"title": title, "url": link})
            else:
                dag_logger.warning(
                    f"Could not extract valid title or link for an element in {url}. Title: '{title}', Link: '{link}'"
                )

        dag_logger.info(f"Successfully extracted {len(articles_data)} articles from {url}.")
        return articles_data

    except requests.exceptions.RequestException as e: 
        dag_logger.error(f"Network or HTTP error fetching {url}: {e}")
        return []
    except Exception as e:
        dag_logger.error(
            f"An unexpected error occurred during scraping {url}: {e}", exc_info=True
        )
        return []
