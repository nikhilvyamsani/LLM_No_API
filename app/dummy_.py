import pandas as pd
import re
import mysql.connector
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
from typing import Tuple

from langchain_community.llms.ollama import Ollama
from langchain_community.utilities import SQLDatabase
from langchain.chains import create_sql_query_chain

# Load environment variables
load_dotenv()

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT")),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE"),
}

encoded_password = quote_plus(MYSQL_CONFIG['password'])

conn = None
db = None
all_tables = []
all_schema = {}
llm = Ollama(model="llama3")

def refresh_db_connection():
    global conn, db, all_tables, all_schema
    if conn:
        conn.close()

    db_uri = (
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:{encoded_password}"
        f"@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
    )
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    db = SQLDatabase.from_uri(db_uri)

    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    all_tables = [row[0] for row in cursor.fetchall()]

    all_schema = {}
    for table in all_tables:
        cursor.execute(f"SHOW COLUMNS FROM {table}")
        columns = cursor.fetchall()
        all_schema[table] = columns

    cursor.close()

# Load once when app starts
if conn is None or db is None:
    try:
        refresh_db_connection()
    except Exception as e:
        print(f"❌ Failed to load DB at startup: {e}")


def list_tables():
    return all_tables

def get_fresh_db():
    return conn, db

def extract_clean_sql_block(text: str) -> str:
    markdown_match = re.search(r'```(?:sql)?\s*(SELECT.*?)```', text, re.DOTALL | re.IGNORECASE)
    if markdown_match:
        return clean_sql_string(markdown_match.group(1))

    sqlquery_match = re.search(r'SQLQuery:\s*(SELECT.*?)(?:\n\n|$|;|```)', text, re.DOTALL | re.IGNORECASE)
    if sqlquery_match:
        return clean_sql_string(sqlquery_match.group(1))

    select_match = re.search(r'(SELECT.*?)(?=\n\n|$|;|```)', text, re.DOTALL | re.IGNORECASE)
    return clean_sql_string(select_match.group(1)) if select_match else ""

def clean_sql_string(sql: str) -> str:
    sql = re.sub(r'`([^`]*)`', r'\1', sql)
    sql = re.sub(r';+$', '', sql)
    sql = re.sub(r'\s+', ' ', sql).strip()
    sql = re.sub(r'^[^A-Za-z]*', '', sql)
    return sql

def ask_llm_and_execute(question: str) -> Tuple[str, pd.DataFrame]:
    global db, conn
    if conn is None or db is None:
        refresh_db_connection()

    print(f"\n💬 Question: {question}")
    cursor = conn.cursor()

    date_column_keywords = {
        "Created_On": ["created", "inserted", "added"],
        "Updated_On": ["updated", "modified", "changed"],
        "Deleted_On": ["deleted", "removed", "erased", "popped"],
        "Audited_On": ["audited", "audits"]
    }

    date_column = None
    for col, keywords in date_column_keywords.items():
        if any(k in question.lower() for k in keywords):
            date_column = col
            break

    schema_str = ""
    for table, columns in all_schema.items():
        schema_str += f"Table: {table}\n"
        for col in columns:
            schema_str += f" - {col[0]} ({col[1]})\n"

    prompt = (
        "donot give this 'Here is the SQL query and result for your question:' kind of text, directly give the sql query without any addition text.\n"
        f"Database schema:\n{schema_str}\n\n"
        f"Important:\n"
        f"always use LIKE for specific date comparisons, but BETWEEN for date ranges.\n"
        f"- Use: {date_column} BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD' for date range queries.\n\n"
        f"Question: {question}\n"
        "Generate MySQL query:"
    )

    sql_chain = create_sql_query_chain(llm, db)
    raw_sql = sql_chain.invoke({"question": prompt}).strip()
    print(f"📄 Raw SQL Output:\n{raw_sql}")

    sql = extract_clean_sql_block(raw_sql)
    print(f"📄 Cleaned SQL Output:\n{sql}")

    cursor.execute(sql)
    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(result, columns=columns)

    return sql, df
