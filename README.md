## RSS Feed Pipeline with Airflow

This project is a data pipeline built with Apache Airflow, designed to create a custom news feed just for you. It gathers the latest articles from various RSS sources, filters them based on your favorite keywords, and keeps you updated with notifications on Telegram and via email.

### Key Features

*   **Database Initialization:** Creates and sets up an SQLite database to store articles and logs.
*   **RSS News Fetching:** Extracts titles and links from a customizable list of RSS sources (`config/news_sources.json`).
*   **Keyword Filtering:** Isolates only the articles that contain the keywords you're interested in, defined in `config/keywords.json`.
*   **Data Storage:** Saves new, relevant articles to the database, avoiding duplicates.
*   **Telegram Notifications:** Sends new articles directly to a Telegram chat of your choice.
*   **Email Notifications:** Notifies you with a summary email upon successful pipeline completion and alerts you if a task fails. `[TBC]`

### Project Structure

```
rss_feed_pipeline/
├── config/
│   ├── keywords.json         # List of keywords for filtering
│   └── news_sources.json     # List of RSS sources
├── data/
│   └── news_feed.db          # SQLite database (created on first run)
├── include/
│   └── ...                   # Python modules with business logic
├── logs/
│   └── ...                   # Task-specific logs
└── main.py                   # Airflow DAG definition
```

### Installation

To get the pipeline running, your Airflow environment needs a few Python libraries. Here’s how to install them:

1.  **Prerequisites:** Make sure you have Python 3.9 or higher installed.
2.  **Virtual Environment (Recommended):** It's always a good practice to create a virtual environment to keep project dependencies isolated.
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows, use: .venv\Scripts\activate
    ```
3.  **Install Dependencies:** From the project's root directory, run this command to install everything you need in one go, thanks to the `pyproject.toml` file:
    ```bash
    pip install .
    ```
4. **Create a personal Telegram Bot where you can receive notifications:** https://core.telegram.org/bots

### Configuration

Before launching the DAG, there are a few simple configuration steps to follow:

1.  **RSS Sources:** Open `config/news_sources.json` and customize the list of sources you want to get news from.
    ```json
    [
        {"name": "ANSA", "url": "https://www.ansa.it/sito/ansait_rss.xml"}
    ]
    ```
2.  **Keywords:** Edit `config/keywords.json` with the list of keywords you want to use for filtering articles.
    ```json
    [
        "data engineer",
        "airflow",
        "python"
    ]
    ```
3.  **Airflow Variables:** Next, you need to provide Airflow with some sensitive data. Go to the Airflow UI, under `Admin -> Variables`, and create these two variables:
    *   `TELEGRAM_BOT_TOKEN`: Your Telegram bot's token.
    *   `TELEGRAM_CHAT_ID`: The ID of the Telegram chat to send notifications to.
4.  **Email Connection (Optional):** If you also want to receive email notifications, make sure your Airflow environment is configured with an SMTP connection, usually named `smtp_default`. [https://airflow.apache.org/docs/apache-airflow/stable/howto/notifications.html]

### Execution

Once the installation and configuration are complete, you're ready to go!

1.  Make sure the project folder (e.g., `rss_feed_pipeline`) is located inside the `dags` directory of your Airflow installation.
2.  From the Airflow UI, enable the `news_feed_pipeline` DAG.
3.  The pipeline is scheduled to run every day at 08:00 UTC, but you can also trigger it manually whenever you want.

---

## RSS Feed Pipeline con Airflow

## Installazione

Per far funzionare la pipeline, il tuo ambiente Airflow ha bisogno di alcune librerie Python. Ecco come installarle:

1.  **Prerequisiti:** Assicurati di avere Python 3.9 o superiore installato.


2.  **Ambiente Virtuale (Consigliato):** È sempre una buona idea creare un ambiente virtuale per mantenere le dipendenze del progetto isolate.
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # Su Windows usa: .venv\Scripts\activate
    ```

3.  **Installazione Dipendenze:** Dalla cartella principale del progetto, esegui questo comando per installare tutto il necessario in un colpo solo, grazie al file `pyproject.toml`:
    ```bash
    pip install .
    ```
4. **Creazione un tuo Bot Telegram dove ricevere le notifiche:** https://core.telegram.org/bots

## Configurazione


Prima di avviare il DAG, ci sono alcuni semplici passaggi di configurazione da seguire:

1.  **Fonti RSS:** Apri `config/news_sources.json` e personalizza l'elenco delle fonti da cui vuoi ricevere notizie.
    ```json
    [
        {"name": "ANSA", "url": "https://www.ansa.it/sito/ansait_rss.xml"}
    ]
    ```
2.  **Parole Chiave:** Modifica `config/keywords.json` con la lista di parole chiave che vuoi usare per filtrare gli articoli.
    ```json
    [
        "data engineer",
        "airflow",
        "python"
    ]
    ```

3.  **Variabili di Airflow:** Successivamente, devi comunicare ad Airflow alcuni dati sensibili. Vai nell'interfaccia di Airflow, in `Admin -> Variables`, e crea queste due variabili:
    *   `TELEGRAM_BOT_TOKEN`: Il token del tuo bot Telegram.
    *   `TELEGRAM_CHAT_ID`: L'ID della chat di Telegram a cui inviare le notifiche.

4.  **Connessione Email (Opzionale):** Se vuoi ricevere anche le notifiche via email, assicurati che il tuo ambiente Airflow sia configurato con una connessione SMTP, solitamente chiamata `smtp_default`. [https://airflow.apache.org/docs/apache-airflow/stable/howto/notifications.html]

## Esecuzione

Una volta completata l'installazione e la configurazione, sei pronto per partire!

1.  Assicurati che la cartella del progetto (es. `rss_feed_pipeline`) si trovi all'interno della directory `dags` della tua installazione di Airflow.
2.  Dall'interfaccia utente di Airflow, attiva il DAG `news_feed_pipeline`.
3.  La pipeline è programmata per essere eseguita ogni giorno alle 08:00 UTC, ma puoi anche avviarla manualmente quando vuoi.