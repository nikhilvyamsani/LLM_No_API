import gradio as gr
import pandas as pd
import mysql.connector
from urllib.parse import quote_plus
from dotenv import load_dotenv
import os
from langchain_ollama import OllamaLLM
from langchain_community.utilities import SQLDatabase  
from langchain.chains import create_sql_query_chain
import re
from typing import Tuple

# Load environment variables
load_dotenv()

# MySQL config from .env
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT")),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE"),
}

# Encode password for URI
encoded_password = quote_plus(MYSQL_CONFIG['password'])

# Table to query
TABLE_NAME = "anomaly_audit"

# LLM config
llm = OllamaLLM(model="llama3")

def get_fresh_db() -> Tuple[mysql.connector.connection.MySQLConnection, SQLDatabase]:
    """Establish fresh database connections."""
    db_uri = (
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:{encoded_password}"
        f"@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
    )
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    db = SQLDatabase.from_uri(db_uri, include_tables=[TABLE_NAME])
    return conn, db

def extract_clean_sql_block(text: str) -> str:
    """Extract and clean SQL from LLM output."""
    # Try markdown code block first
    markdown_match = re.search(r'```(?:sql)?\s*(SELECT.*?)```', text, re.DOTALL | re.IGNORECASE)
    if markdown_match:
        return clean_sql_string(markdown_match.group(1))
    
    # Fallback to SQLQuery: prefix
    sqlquery_match = re.search(r'SQLQuery:\s*(SELECT.*?)(?:\n\n|$|;|```)', text, re.DOTALL | re.IGNORECASE)
    if sqlquery_match:
        return clean_sql_string(sqlquery_match.group(1))
    
    # Final fallback - find first SELECT
    select_match = re.search(r'(SELECT.*?)(?=\n\n|$|;|```)', text, re.DOTALL | re.IGNORECASE)
    return clean_sql_string(select_match.group(1)) if select_match else ""

def clean_sql_string(sql: str) -> str:
    """Clean and normalize SQL string."""
    sql = re.sub(r'`([^`]*)`', r'\1', sql)  # Remove backticks
    sql = re.sub(r';+$', '', sql)           # Remove trailing semicolons
    sql = re.sub(r'\s+', ' ', sql).strip()   # Normalize whitespace
    sql = re.sub(r'^[^A-Za-z]*', '', sql)    # Remove leading non-alphabet chars
    return sql

def ask_llm_and_execute(question: str) -> Tuple[str, pd.DataFrame]:
    """Main function to execute queries."""
    print(f"\n💬 Question: {question}")
    conn, db = get_fresh_db()

    try:
        # Get schema
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE {TABLE_NAME}")
        schema_info = cursor.fetchall()

        # Define keyword mappings for date columns
        date_column_keywords = {
            "created_on": ["created", "inserted", "added"],
            "updated_on": ["updated", "modified", "changed"],
            "deleted_on": ["deleted", "removed", "erased", "popped"],
            "audited_on": ["audited", "audit"]
        }

        # Determine the appropriate date column based on the question
        date_column = None
        for col, keywords in date_column_keywords.items():
            if any(keyword in question.lower() for keyword in keywords):
                date_column = col
                break

        if not date_column:
            # Default to 'created_on' if no specific date column is identified
            date_column = "created_on"

        # Enhanced prompt with exact schema info
        enhanced_prompt = (
            f"donot give this 'Here is the SQL query and result for your question:' kind of text, directly give the sql query without any addition text, since we are going to run your output as query \n"
            f"Database schema for {TABLE_NAME}:\n{schema_info}\n\n"
            f"Important:\n"
            f"- Date column is '{date_column}' (NOT 'created_at')\n"
            f"- For date ranges use: {date_column} BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'\n\n"
            f"Question: {question}\n"
            "Generate MySQL query:"
        )

        # Generate and clean SQL
        sql_chain = create_sql_query_chain(llm, db)
        raw_sql = sql_chain.invoke({"question": enhanced_prompt}).strip()
        print(f"📄 Raw SQL Output:\n{raw_sql}")
        
        sql = extract_clean_sql_block(raw_sql)
        print(f"📄 Cleaned SQL Output:\n{sql}")
        
        # Execute query
        cursor.execute(sql)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=columns)
        return sql, df

    except Exception as e:
        raise ValueError(f"SQL Execution Error: {e}")
    finally:
        conn.close()

# Gradio UI
with gr.Blocks() as demo:
    gr.Markdown("# Ask Your Database (LLaMA3 + MySQL)")
    question_input = gr.Textbox(label="Enter your question")
    sql_output = gr.Textbox(label="Generated SQL Query")
    result_output = gr.Dataframe(label="Query Results")

    def process_question(question):
        try:
            sql, df = ask_llm_and_execute(question)
            return sql, df
        except Exception as e:
            return str(e), pd.DataFrame()

    question_input.submit(process_question, inputs=question_input, outputs=[sql_output, result_output])

demo.launch()