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
SITE_DB = "seekright_v3"
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
    # Remove backticks
    sql = re.sub(r'`([^`]*)`', r'\1', sql)

    # Fix common bad patterns like CURDATE() + '%' -> LIKE CURDATE()%
    sql = re.sub(r'CURDATE\(\)\s*\+\s*["\']%["\']', r"LIKE CONCAT(CURDATE(), '%')", sql)

    # Remove trailing semicolons
    sql = re.sub(r';+$', '', sql)

    # Normalize whitespace
    sql = re.sub(r'\s+', ' ', sql).strip()

    # Remove any leading non-alphabet characters (like punctuation)
    sql = re.sub(r'^[^A-Za-z]*', '', sql)

    return sql


def create_joined_view() -> None:
    """Create or replace the joined view across sr_lnt and seekright_v3 databases."""
    conn = get_db_connection(ANOMALY_DB)  # This connects to sr_lnt
    cursor = conn.cursor()

    try:
        # Drop the view from sr_lnt
        cursor.execute(f"DROP VIEW IF EXISTS {ANOMALY_DB}.{JOINED_VIEW}")

        # Create the joined view in sr_lnt
        create_view_sql = f"""
        CREATE VIEW {ANOMALY_DB}.{JOINED_VIEW} AS
        SELECT a.*, s.site_name as site_name
        FROM {ANOMALY_DB}.{ANOMALY_TABLE} a
        LEFT JOIN {SITE_DB}.{SITE_TABLE} s ON a.site_id = s.site_id
        """

        cursor.execute(create_view_sql)
        conn.commit()
        print("✅ Joined view created successfully.")
    
    except Exception as e:
        print(f"❌ Failed to create view: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

def get_view_schema() -> List[tuple]:
    """Get the schema of the joined view."""
    # First ensure the view exists
    create_joined_view()
    
    conn = get_db_connection(ANOMALY_DB)
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE {ANOMALY_DB}.{JOINED_VIEW}")
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
            "audited_on": ["audited", "audits done"]
        }

        # Determine the appropriate date column based on the question
        date_column = None
        for col, keywords in date_column_keywords.items():
            if any(keyword in question.lower() for keyword in keywords):
                date_column = col
                break
        
        # Enhanced prompt with joined view schema
        enhanced_prompt = (
    f"Generate only a valid MySQL query—no explanations, no markdown, no extra text. "
    f"This output will be executed directly.\n\n"

    f"Use only the joined view named `{JOINED_VIEW}`. This view merges all columns from `sr_lnt.anomaly_audit` "
    f"with an additional column `site_name` from `seekright_v3.tbl_site`.\n\n"

    f"Full Schema of `{JOINED_VIEW}`:\n{view_schema}\n\n"

    f"Instructions:\n"
    f"- Never use original table names — only use `{JOINED_VIEW}`.\n"
    f"- Do not add filters like deleted = 0 unless explicitly asked.\n"
    f"- Only use columns listed in the above schema.\n"
    f"- For TP/FP analysis, refer to `Audit_status` (1 = TP, 0 = FP).\n"
    f"- Date-related columns: `Audited_On`, `video_date`, `Created_On`. Choose the correct one based on the query context.\n"
    f"- For date filters:\n"
    f"  - If the question refers to \"today\" or \"now\", use `Audited_On LIKE CONCAT(CURDATE(), '%')` to filter by today's date.\n"
    f"  - If the user specifies an exact date (e.g., '2024-04-01'), use `Audited_On LIKE 'YYYY-MM-DD%'`.\n"
    f"  - If a date range is given, use `Audited_On BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'`.\n"
    f"  - Do not default to today's date unless explicitly mentioned.\n"
    f"- Use `site_name` for site name filtering, and `site_id` for ID-based filters.\n"
    f"- Never guess column names — rely strictly on the schema provided above.\n"
    f"- Use only columns that are present in the joined view schema. Do not assume the existence of columns not listed.\n"
    f"- The input question might include audit-specific metrics, site-level filtering, or date filters — infer intent precisely and construct the query accordingly.\n\n"
    f"Question: {question}\n"
    "Output MySQL query:"
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
    return [f"{ANOMALY_DB}.{ANOMALY_TABLE}", f"{SITE_DB}.{SITE_TABLE}", f"{ANOMALY_DB}.{JOINED_VIEW}"]

def get_fresh_db() -> None:
    """Initialize the database schema on app start or refresh."""
    # Ensure the joined view is created and up to date
    create_joined_view()
    print("🆕 Fresh database schema initialized.")

# Call get_fresh_db() on app start or when refreshing the schema
# if __name__ == "__main__":
#     # Initialize fresh database schema
#     get_fresh_db()
    
#     # Example query using the joined view
#     sql, results = ask_llm_and_execute("Show me anomalies from the SRTL site created last week")
#     print(results)
