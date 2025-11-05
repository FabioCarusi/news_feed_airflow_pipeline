"""
This module provides a custom logging handler for SQLite databases,
allowing Airflow task logs to be stored directly in a specified database.

It includes:
- 'DBLogHandler': A 'logging.Handler' subclass that writes log records
  to a SQLite table.
- 'task_db_logger': A context manager to temporarily attach and detach
  the 'DBLogHandler' to the root logger for the duration of an Airflow task.
"""

import logging
import sqlite3
from contextlib import contextmanager
import sys
from typing import Optional
from airflow.models.taskinstance import TaskInstance


class DBLogHandler(logging.Handler):
    """
    Un gestore di log personalizzato che scrive i record in un database SQLite.
    Si integra con il framework di logging standard di Python.
    """

    def __init__(self, conn: sqlite3.Connection, dag_id: str, task_id: str):
        super().__init__()
        self.conn = conn
        self.dag_id = dag_id
        self.task_id = task_id
        self.cursor = self.conn.cursor()

    def emit(self, record: logging.LogRecord):
        """
        Scrive un record di log nel database.
        Questo metodo viene chiamato automaticamente dal framework di logging.
        """
        # La connessione e il commit sono gestiti dal context manager
        try:
            message = self.format(record)
            self.cursor.execute(
                """
                INSERT INTO logs (level, message, dag_id, task_id)
                VALUES (?, ?, ?, ?)
                """,
                (record.levelname, message, self.dag_id, self.task_id),
            )
        except (sqlite3.DatabaseError, sqlite3.OperationalError, sqlite3.IntegrityError) as e:
            print(
                f"CRITICAL: Impossibile scrivere log su database: {e}", file=sys.stderr
            )
            raise

@contextmanager
def task_db_logger(db_path: str, ti: Optional[TaskInstance] = None):
    """
    Un context manager per aggiungere e rimuovere temporaneamente un DBLogHandler
    al logger root per la durata di un task.
    Fornisce la connessione al database per essere usata all'interno del blocco 'with'.
    """
    if not ti:
        raise ValueError(
            "TaskInstance (ti) è necessario per ottenere il contesto del DAG."
        )

    conn = None
    db_handler = None
    try:
        # Aumenta il timeout per ridurre gli errori "database is locked"
        conn = sqlite3.connect(db_path, timeout=15)
        root_logger = logging.getLogger()

        db_handler = DBLogHandler(conn=conn, dag_id=ti.dag_id, task_id=ti.task_id)
        root_logger.addHandler(db_handler)

        yield conn  # Fornisce l'oggetto connessione

        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error("Errore nel context manager del logger DB: %s", e, exc_info=True)
        raise
    finally:
        if db_handler:
            logging.getLogger().removeHandler(db_handler)
        if conn:
            conn.close()
