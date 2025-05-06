import os
import re
import mysql.connector
import pandas as pd
from dotenv import load_dotenv
from urllib.parse import quote_plus
from typing import Tuple, List

from langchain_community.llms.ollama import Ollama
from langchain_community.utilities import SQLDatabase
from langchain.chains import create_sql_query_chain
from pandasql import sqldf

# Load environment variables
load_dotenv()

# MySQL connection config
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT")),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
}

# Constants
SITE_DB = "seekright_v3"
SITE_TABLE = "tbl_site"
USER_TABLE = "tbl_user"
ANOMALY_TABLE = "anomaly_audit"
llm = Ollama(model="llama3")

# Dynamic anomaly DB (set via dropdown/org selector)
ANOMALY_DB = None

def set_anomaly_db(anomaly_db: str):
    global ANOMALY_DB
    ANOMALY_DB = anomaly_db

def get_db_connection(database: str = None):
    config = MYSQL_CONFIG.copy()
    if database:
        config["database"] = database
    return mysql.connector.connect(**config)

def create_db_uri(database: str) -> str:
    encoded_password = quote_plus(MYSQL_CONFIG['password'])
    return (
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:{encoded_password}"
        f"@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{database}"
    )

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
    sql = re.sub(r'CURDATE\(\)\s*\+\s*["\']%["\']', r"LIKE CONCAT(CURDATE(), '%')", sql)
    sql = re.sub(r';+$', '', sql)
    sql = re.sub(r'\s+', ' ', sql).strip()
    sql = re.sub(r'^[^A-Za-z]*', '', sql)
    return sql

def get_joined_schema_description() -> str:
    conn = get_db_connection(ANOMALY_DB)
    cursor = conn.cursor()

    def describe_table(db: str, table: str):
        cursor.execute(f"DESCRIBE {db}.{table}")
        return cursor.fetchall()

    try:
        anomaly_schema = describe_table(ANOMALY_DB, ANOMALY_TABLE)
        site_schema = describe_table(SITE_DB, SITE_TABLE)
        user_schema = describe_table(SITE_DB, USER_TABLE)

        schema_lines = []
        schema_lines.append(f"From `{ANOMALY_DB}.{ANOMALY_TABLE}`:")
        schema_lines += [f"- {col[0]} ({col[1]})" for col in anomaly_schema]
        schema_lines.append(f"\nFrom `{SITE_DB}.{SITE_TABLE}`:")
        schema_lines += [f"- {col[0]} ({col[1]})" for col in site_schema]
        schema_lines.append(f"\nFrom `{SITE_DB}.{USER_TABLE}`:")
        schema_lines += [f"- {col[0]} ({col[1]})" for col in user_schema]
        return "\n".join(schema_lines)
    finally:
        cursor.close()
        conn.close()

def ask_llm_and_execute(question: str) -> Tuple[str, pd.DataFrame]:
    print(f"\n💬 Question: {question}")

    db_uri = create_db_uri(ANOMALY_DB)
    db = SQLDatabase.from_uri(db_uri)

    schema_description = get_joined_schema_description()

    prompt = (f"""
You are an expert MySQL query writer.
The database has the following structure with these tables:

{schema_description}

To answer the question, use the following JOIN inline in your query:
FROM {ANOMALY_DB}.{ANOMALY_TABLE} a LEFT JOIN {SITE_DB}.{SITE_TABLE} s 
ON a.site_id = s.site_id LEFT JOIN {SITE_DB}.{USER_TABLE} u 
ON a.audited_user_id = u.user_id

Use aliases: a for anomaly_audt, s for tbl_site, u for tbl_user.

Instructions:
- Only use columns listed in the schema above. Never guess column names.
- Use only relevant columns for the specific question.
- **Do not use `auto_audit` or `false_audit` unless specifically asked**. 
    - These columns are only relevant if the question explicitly asks about the audit type (auto, manual, or false audit).
    - For **TP/FP analysis**, use `Audit_status`:
      - TP: Audit_status = 1
      - FP: Audit_status = 0
- If the question asks for an **audit count by site**, only use the necessary columns (like `site_name` and relevant date columns).
- For date filtering, always refer to `video_date`, `Audited_On`, or `Created_On` depending on the context (unless specified otherwise).
f"\nUse the following logic to determine audit type:\n"
f"- manual_audit: auto_audit = 0\n"
f"- auto_audit: auto_audit = 1\n"
f"- false_audit: false_audit = 1\n"
Date Filtering Instructions:
- For questions about TPs and FPs:
  - Always apply any date filter on the `video_date` column.
- For other questions (not about TPs and FPs):
  - Use `Audited_On`, `video_date`, or `Created_On` based on the question context.
- Date filters examples:
  - If the question says 'today' or 'now', use `video_date LIKE CONCAT(CURDATE(), '%')`.
  - If the question specifies an exact date like '2024-04-01', use `video_date LIKE 'YYYY-MM-DD%'`.
  - If the question specifies a date range, use `video_date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'`.
  - Never assume today's date unless the user explicitly mentions it.

Other Instructions:
- Use `site_name` for site-level filters, and `site_id` for ID-based filters.
- Never add deleted = 0 or any similar condition unless specifically asked.
- Strictly follow the provided schema — no assumptions.

Question: {question}
Output MySQL query:
""")

    sql_chain = create_sql_query_chain(llm, db)
    raw_sql = sql_chain.invoke({"question": prompt}).strip()
    print(f"📄 Raw SQL Output:\n{raw_sql}")

    sql = extract_clean_sql_block(raw_sql)
    print(f"📄 Cleaned SQL Output:\n{sql}")

    conn = get_db_connection(ANOMALY_DB)
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return sql, pd.DataFrame(result, columns=columns)
    finally:
        conn.close()

def list_tables() -> List[str]:
    return [f"{ANOMALY_DB}.{ANOMALY_TABLE}", f"{SITE_DB}.{SITE_TABLE}", f"{SITE_DB}.{USER_TABLE}"]

