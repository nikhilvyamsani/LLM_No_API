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

# Load environment variables
load_dotenv()

# MySQL connection config (without database)
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT")),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
}

# Database and table definitions
ANOMALY_DB = "sr_lnt"
ANOMALY_TABLE = "anomaly_audit"
SITE_DB = "seekright_v3_poc"
SITE_TABLE = "tbl_site"
JOINED_VIEW = "joined_anomaly_view"  

# Encode password for URI
encoded_password = quote_plus(MYSQL_CONFIG['password'])

# LLM config
llm = Ollama(model="llama3")

def get_db_connection(database: str = None) -> mysql.connector.connection.MySQLConnection:
    """Establish a database connection."""
    config = MYSQL_CONFIG.copy()
    if database:
        config["database"] = database
    return mysql.connector.connect(**config)

def create_db_uri(database: str) -> str:
    """Create a database URI for SQLDatabase."""
    return (
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:{encoded_password}"
        f"@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{database}"
    )

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

def create_joined_view() -> None:
    """Create or replace the joined view."""
    conn = get_db_connection(ANOMALY_DB)
    cursor = conn.cursor()
    
    # Drop the view if it exists
    cursor.execute(f"DROP VIEW IF EXISTS {JOINED_VIEW}")
    
    # Create the joined view
    create_view_sql = f"""
    CREATE VIEW {JOINED_VIEW} AS
    SELECT a.*, s.site_name as site_name
    FROM {ANOMALY_DB}.{ANOMALY_TABLE} a
    LEFT JOIN {SITE_DB}.{SITE_TABLE} s ON a.site_id = s.site_id
    """
    
    cursor.execute(create_view_sql)
    conn.commit()
    conn.close()

def get_view_schema() -> List[tuple]:
    """Get the schema of the joined view."""
    # First ensure the view exists
    create_joined_view()
    
    conn = get_db_connection(ANOMALY_DB)
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE {JOINED_VIEW}")
    schema = cursor.fetchall()
    conn.close()
    return schema

def ask_llm_and_execute(question: str) -> Tuple[str, pd.DataFrame]:
    """Main function to execute queries using the joined view."""
    print(f"\n💬 Question: {question}")
    
    # Make sure the joined view exists
    create_joined_view()
    
    # Connect to the database
    conn = get_db_connection(ANOMALY_DB)
    
    try:
        # Get schema for the joined view
        view_schema = get_view_schema()
        
        # Create SQLDatabase for LangChain
        db_uri = create_db_uri(ANOMALY_DB)
        db = SQLDatabase.from_uri(db_uri)
        
        # Define keyword mappings for date columns
        date_column_keywords = {
            "Created_On": ["created", "inserted", "added"],
            "updated_on": ["updated", "modified", "changed"],
            "deleted_on": ["deleted", "removed", "erased", "popped"],
            "audited_on": ["audited", "audits"]
        }

        # Determine the appropriate date column based on the question
        date_column = None
        for col, keywords in date_column_keywords.items():
            if any(keyword in question.lower() for keyword in keywords):
                date_column = col
                break
        
        # Enhanced prompt with joined view schema
        enhanced_prompt = (
            f"Don't include any explanatory text, just the SQL query. I'll run your output directly as a query.\n\n"
            f"I've created a joined view called '{JOINED_VIEW}' that contains all data from both tables.\n"
            f"This view includes all columns from anomaly_audit plus an additional 'site_name' column, read all the column name and generate querries accordingly\n\n"
            f"Joined View Schema ({JOINED_VIEW}):\n{view_schema}\n\n"
            f"Important notes:\n"
            f"- For querries like TPs and FPs, use Audit_status col,like if its 1 its TP else if 0 its FP, and for queries like total audits or audits done consider IsAudited "
            f"- You may be asked for mixed kind of queries, be careful while processing"
            f"- Always query from the {JOINED_VIEW} view instead of the original tables\n"
            f"- You can use site_name for filtering by site names and site_id for filtering by IDs\n"
            f"- For date comparisons, use LIKE instead of = for specific date queries and for today always use curr_date wiht proper col names like Audited_On\n"
            f"- For date ranges use: {date_column} BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD' if appropriate\n\n"
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
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=columns)
        return sql, df

    except Exception as e:
        raise ValueError(f"SQL Execution Error: {e}")
    finally:
        conn.close()

def list_tables() -> List[str]:
    """Return available tables for UI."""
    return [f"{ANOMALY_DB}.{ANOMALY_TABLE}", f"{SITE_DB}.{SITE_TABLE}", JOINED_VIEW]

def get_fresh_db() -> None:
    """Initialize the database schema on app start or refresh."""
    # Ensure the joined view is created and up to date
    create_joined_view()
    print("🆕 Fresh database schema initialized.")

# Call get_fresh_db() on app start or when refreshing the schema
if __name__ == "__main__":
    # Initialize fresh database schema
    get_fresh_db()
    
    # Example query using the joined view
    sql, results = ask_llm_and_execute("Show me anomalies from the SRTL site created last week")
    print(results)
