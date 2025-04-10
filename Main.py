# backend.py
import pandas as pd
import re
import mysql.connector
import os
from dotenv import load_dotenv
from langchain_community.llms.ollama import Ollama
from langchain_community.utilities import SQLDatabase  
from langchain.chains import create_sql_query_chain

# 🔐 Load environment variables
load_dotenv()

# ✅ Load MySQL config from .env
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT")),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE"),
}
db_uri = f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
print("DB URI:", db_uri)

# ✅ Only include one table (optional)
TABLE_NAME = "anomaly_audit"

# ✅ LLM config
llm = Ollama(model="llama3")

# ✅ Establish MySQL connection and LangChain SQLDatabase
def get_fresh_db():
    db_uri = (
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}"
        f"@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
    )
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    db = SQLDatabase.from_uri(db_uri, include_tables=[TABLE_NAME])
    return conn, db
import pandas as pd
import re
import mysql.connector
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

from langchain_community.llms.ollama import Ollama
from langchain_community.utilities import SQLDatabase  
from langchain.chains import create_sql_query_chain

# 🔐 Load environment variables
load_dotenv()

# ✅ Load MySQL config from .env
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT")),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE"),
}

# ✅ Encode password for URI
encoded_password = quote_plus(MYSQL_CONFIG['password'])

# ✅ Only include one table (optional)
TABLE_NAME = "anomaly_audit"

# ✅ LLM config
llm = Ollama(model="llama3")

# ✅ Establish MySQL connection and LangChain SQLDatabase
def get_fresh_db():
    db_uri = (
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:{encoded_password}"
        f"@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
    )
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    db = SQLDatabase.from_uri(db_uri, include_tables=[TABLE_NAME])
    return conn, db

# ✅ Ask LLM → convert to SQL → validate and run
def ask_llm_and_execute(question: str):
    print(f"\n💬 Question: {question}")
    conn, db = get_fresh_db()

    try:
        sql_chain = create_sql_query_chain(llm, db)
        sql = sql_chain.invoke({"question": question}).strip()
        print(f"📄 LLM SQL Output:\n{sql}")

        match = re.search(r"(SELECT[\s\S]+)", sql, re.IGNORECASE)
        if not match:
            raise ValueError("No valid SELECT statement found.")

        sql = match.group(1).strip().rstrip(";")

        if not sql.lower().startswith("select"):
            raise ValueError("Only SELECT queries are allowed (read-only).")

        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return sql, pd.DataFrame(rows, columns=columns)

    except Exception as e:
        raise ValueError(f"SQL Execution Error: {e}")

    finally:
        conn.close()

# ✅ Required by UI to display available tables
def list_tables():
    return [TABLE_NAME]

# ✅ Ask LLM → convert to SQL → validate and run
def ask_llm_and_execute(question: str):
    print(f"\n💬 Question: {question}")
    conn, db = get_fresh_db()

    try:
        sql_chain = create_sql_query_chain(llm, db)
        sql = sql_chain.invoke({"question": question}).strip()
        print(f"📄 LLM SQL Output:\n{sql}")

        match = re.search(r"(SELECT[\s\S]+)", sql, re.IGNORECASE)
        if not match:
            raise ValueError("No valid SELECT statement found.")

        sql = match.group(1).strip().rstrip(";")

        if not sql.lower().startswith("select"):
            raise ValueError("Only SELECT queries are allowed (read-only).")

        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return sql, pd.DataFrame(rows, columns=columns)

    except Exception as e:
        raise ValueError(f"SQL Execution Error: {e}")

    finally:
        conn.close()

# ✅ Required by UI to display available tables
def list_tables():
    return [TABLE_NAME]
