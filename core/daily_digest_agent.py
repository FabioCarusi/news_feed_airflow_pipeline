"""Definisce il modello AI"""

import logging
import json
from datetime import datetime

from typing import Any
import requests
from bs4 import BeautifulSoup
from openai import OpenAI

logger = logging.getLogger(__name__)

MAX_CHARS = 8000  # per evitare di far esplodere i token


class DailyDigestAgent:
    """
    Agent per la generazione di un daily digest di notizie tecniche.
    Utilizza un modello OpenAI per analizzare gli articoli e produrre un riassunto strutturato.
    """

    def __init__(
        self,
        api_key: str,
        model_name: str,
        base_url: str = "https://openrouter.ai/api/v1",
    ):
        self.client = OpenAI(base_url=base_url, api_key=api_key)
        self.model_name = model_name
        self.agent_instructions = self._build_instructions()
        self.tools = self._build_tools()

    def run_daily_digest_agent(self, articles: list[dict], date_str: str) -> str:
        """
        Riceve un json con le notizie filtrate e le passa all'agente
        articles: lista di dict RSS (title, url, summary, source, matched_keywords, ...)
        date_str: "YYYY-MM-DD" (Airflow ds)
        """
        if not articles:
            dt = datetime.strptime(date_str, "%Y-%m-%d").date()
            date_human = dt.strftime("%d %B %Y")
            return (
                f"<b>Your Daily News Feed - {date_human}</b>\n\n"
                "Nessun nuovo articolo interessante trovato oggi."
            )

        dt = datetime.strptime(date_str, "%Y-%m-%d").date()
        date_human = dt.strftime("%d %B %Y")

        agent_articles = [
            {
                "title": a.get("title", ""),
                "url": a.get("url", ""),
                "summary": a.get("summary", ""),
                "source": a.get("source", ""),
                "matched_keywords": a.get("matched_keywords", []),
            }
            for a in articles
        ]

        payload = {"date": date_human, "articles": agent_articles}
        return self._call_openai_agent(payload)

    def _call_openai_agent(self, payload: dict) -> str:
        """
        Gestisce le chiamate al modello
        """
        user_content = (
            "Ecco i dati per il daily digest (JSON):\n\n"
            f"{json.dumps(payload, ensure_ascii=False)}"
        )

        # Prima chiamata
        response = self.client.chat.completions.create(
            model=self.model_name,
            messages=[
                {"role": "system", "content": self.agent_instructions},
                {"role": "user", "content": user_content},
            ],
            tools=self.tools,
            tool_choice="auto",
            temperature=0.3,
        )

        msg = response.choices[0].message

        # Nessun tool: risposta finale
        if not getattr(msg, "tool_calls", None):
            return f"Nessun tool trovato: {msg.content}"

        # Eseguiamo i tool richiesti
        tool_messages = []
        for tool_call in msg.tool_calls or []:
            fn_name = tool_call.function.name
            fn_args = json.loads(tool_call.function.arguments)

            if fn_name == "get_article_content":
                tool_result = self._get_article_content(
                    url=fn_args["url"],
                    title=fn_args.get("title", ""),
                    summary=fn_args.get("summary", ""),
                )
            else:
                tool_result = {"error": f"Unknown tool {fn_name}"}

            tool_messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "name": fn_name,
                    "content": json.dumps(tool_result, ensure_ascii=False),
                }
            )

        # Seconda chiamata: istruzioni + stesso payload + risultati tool
        second_user_content = (
            user_content
            + "\n\nHo eseguito i tool che hai richiesto. Usa i risultati per creare il daily digest."
        )

        second_messages = [
            {"role": "system", "content": self.agent_instructions},
            {"role": "user", "content": second_user_content},
            *tool_messages,
        ]

        final_response = self.client.chat.completions.create(
            model=self.model_name,
            messages=second_messages,
            temperature=0.3,
        )

        return final_response.choices[0].message.content or "Nessuna risposta"

    def _extract_main_text(self, html: str) -> str:
        """Extract text from HTML."""
        soup = BeautifulSoup(html, "html.parser")
        paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
        text = "\n\n".join(p for p in paragraphs if p)
        return text or soup.get_text(" ", strip=True)

    def _get_article_content(self, url: str, title: str, summary: str) -> dict:
        """Extract full text from an article URL."""
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"
            )
        }

        try:
            r = requests.get(url, headers=headers, timeout=60)
        except requests.RequestException as e:
            logger.error("Errore nella richiesta: %s", e)
            return {
                "url": url,
                "status": "error",
                "title": title,
                "summary": summary,
                "text": "",
                "error_message": str(e),
            }

        if r.status_code != 200:
            logger.error(
                "Impossibile recuperare il testo. Status code: %s", r.status_code
            )
            return {
                "url": url,
                "status": "error",
                "title": title,
                "summary": summary,
                "text": "",
                "error_message": f"status_code={r.status_code}",
            }

        html = r.text
        text = self._extract_main_text(html)
        if len(text) > MAX_CHARS:
            text = text[:MAX_CHARS] + "..."

        return {
            "url": url,
            "status": "ok",
            "title": title,
            "summary": summary,
            "text": text,
            "error_message": None,
        }

    @staticmethod
    def _build_instructions() -> str:
        return """
    Sei un agente che crea un daily digest di notizie tecniche per uno sviluppatore/data engineer.

    Ricevi:
    - la data del digest
    - una lista di articoli del giorno, ognuno con:
    - title
    - url
    - summary (da RSS)
    - source
    - matched_keywords

    Hai a disposizione un tool chiamato `get_article_content` che, dato un URL, scarica la pagina e restituisce:
    - url
    - status ("ok" oppure "error")
    - title (se disponibile)
    - summary (riassunto RSS passato in input al tool)
    - text (contenuto principale dell'articolo in testo semplice)
    - error_message (se si verifica un problema)

    Compito:

    1. Per ogni articolo, valuta se il riassunto RSS è sufficiente a capirne il contenuto.
    2. Se il riassunto è troppo generico, ambiguo o insufficiente, usa il tool `get_article_content` per leggere il testo completo.
    3. Se il tool restituisce status="error" o text vuoto, continua a usare solo il riassunto RSS.
    4. Usa sia i riassunti RSS sia il contenuto completo che hai letto per identificare i temi principali della giornata.
    5. Produci un daily digest in italiano con questa struttura:

    - Una breve introduzione (2–3 frasi) che descrive il quadro generale della giornata.
    - Una lista di 3–7 bullet point, ognuna con:
        - Un titolo conciso del tema.
        - Una breve spiegazione (1–2 frasi).
    - Una sezione finale 'Per approfondire' con l'elenco degli articoli, uno per riga, nel formato:
        TITOLO ORIGINALE – URL

    Regole:
    - Non inventare articoli o URL.
    - Non tradurre i titoli: mantieni i titoli originali.
    - Puoi ignorare articoli troppo marginali o ripetitivi.
    - Rispondi sempre in italiano.
    """.strip()

    @staticmethod
    def _build_tools() -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": "get_article_content",
                    "description": "Scarica e restituisce il contenuto principale di un articolo dato il suo URL.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "url": {
                                "type": "string",
                                "description": "URL dell'articolo da leggere.",
                            },
                            "title": {
                                "type": "string",
                                "description": "Titolo dell'articolo (da RSS).",
                            },
                            "summary": {
                                "type": "string",
                                "description": "Riassunto RSS dell'articolo.",
                            },
                        },
                        "required": ["url", "title", "summary"],
                    },
                },
            }
        ]
