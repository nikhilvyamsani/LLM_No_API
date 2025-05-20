import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import re
import pandasql as ps
from langchain_community.llms.ollama import Ollama
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain

# Load environment variables
load_dotenv()

# Constants
SITE_DB = "seekright_v3"
SITE_TABLE = "tbl_site"
USER_TABLE = "tbl_user"
ANOMALY_TABLE = "anomaly_audit"
STATICS_TABLE = "tbl_site_statics"

# Cached global dataframe
global_df = None
global_site_statics_df = None
global_site_df = None
combined_df = None


def get_mysql_connection():
    """Establish MySQL connection using .env credentials."""
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD")
    )
def get_all_usernames() -> list:
    """Returns a list of unique audited usernames from the loaded global DataFrame."""
    global global_df
    if global_df is None or global_df.empty:
        return []
    return sorted(global_df['username'].dropna().unique().tolist())

def get_all_asset_names() -> list:
    """Returns a list of unique asset names from the loaded global DataFrame."""
    global global_df
    if global_df is None or global_df.empty:
        return []
    return sorted(global_df['Asset'].dropna().unique().tolist())


def load_and_join_data(anomaly_db: str) -> pd.DataFrame:
    """Loads anomaly + site + user data into a single joined DataFrame."""
    global global_df
    conn = get_mysql_connection()

    query = f"""
    SELECT 
        a.*, 
        s.site_name, 
        u.username 
    FROM {anomaly_db}.{ANOMALY_TABLE} a
    LEFT JOIN {SITE_DB}.{SITE_TABLE} s ON a.site_id = s.site_id
    LEFT JOIN {SITE_DB}.{USER_TABLE} u ON a.audited_user_id = u.user_id
    """

    df = pd.read_sql(query, conn)
    conn.close()

    global_df = df  # Cache the global DataFrame for later use
    return df

def load_site_statics_data(anomaly_db: str) -> pd.DataFrame:
    """Loads site statics data into a DataFrame."""
    global global_site_statics_df
    conn = get_mysql_connection()
    query = f"""
    SELECT ss.*, s.site_name FROM  {anomaly_db}.{STATICS_TABLE} ss
    LEFT JOIN {SITE_DB}.{SITE_TABLE} s ON ss.site_id = s.site_id
    """
    df = pd.read_sql(query, conn)
    conn.close()

    global_site_statics_df = df 
    return df

def load_all_site_statics(org_db_map: dict[str, str]) -> pd.DataFrame:
    """
    Load site statics data from multiple org DBs and add 'org_name' column.

    Args:
        org_db_map (dict): Keys are org names, values are anomaly DB names.

    Returns:
        pd.DataFrame: Combined DataFrame with org_name column
    """
    all_dfs = []

    for org_name, anomaly_db in org_db_map.items():
        conn = get_mysql_connection()
        cursor = conn.cursor(dictionary=True)

        query = f"""
        SELECT ss.*, s.site_name 
        FROM {anomaly_db}.{STATICS_TABLE} ss
        LEFT JOIN {SITE_DB}.{SITE_TABLE} s ON ss.site_id = s.site_id
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
        df.insert(0, "org_name", org_name)  # Add org_name as first column

        all_dfs.append(df)
        cursor.close()
        conn.close()

    combined_df = pd.concat(all_dfs, ignore_index=True)
    if 'plaza' in combined_df.columns and 'Plaza' in combined_df.columns:
        combined_df['plaza'] = combined_df['plaza'].combine_first(combined_df['Plaza'])
        combined_df.drop(columns=['Plaza'], inplace=True)

    global global_site_statics_df
    global_site_statics_df = combined_df

    return combined_df



def generate_schema_description(df: pd.DataFrame) -> str:
    """Converts DataFrame schema to a string format for LLM prompt."""
    return "\n".join([f"- {col}: {df[col].dtype}" for col in df.columns])

def build_query_prompt_statics (schema_description: str, question: str) -> str:
    """Builds the prompt for SQL generation based on schema and question."""
    return f"""
You are a helpful SQL assistant that generates valid SQL queries from natural language questions.
You have access to a SQL table called `df` with the following schema:   
{schema_description}
Instructions:
- Only use columns listed in the schema above. Never guess column names.
- Use only relevant columns for the specific question.
-To check for videos proccessed on a date or date range , use video_date for the given date input and is_processed =1 and progess_value = 100.
-for date related queries
  - Never use 'between' keyword for filtering for given date rage (like from and to dates),follow below:
    - If the question specifies an exact date like '2024-04-01', use for ex like : `video_date LIKE 'YYYY-MM-DD%'`.
    - If the question specifies a date range, use for example: `video_date >= 'YYYY-MM-DD' AND video_date <= 'YYYY-MM-DD' .
-if asked for each site,use group by site name
- to check whether the videos are processed, use progress_value = 100 ->processed else not processed.
- the below are the col descriptions :
 - video_date  : date when video is recorded or taken
 - processed_on : date when video is processed by client or uploaded to the tool for processing
 - is_processed : 1 -> video is processed, 0 -> video is not processed
 - progress_value : 100 -> video is completely processed, else not processed
 Question : 
 {question}
 Respond with only the SQL query.


"""



def build_query_prompt_audits(schema_description: str, question: str) -> str:
    """Builds the prompt for SQL generation based on schema and question."""
    return f"""
You are a helpful SQL assistant that generates valid SQL queries from natural language questions.
You have access to a SQL table called `df` with the following schema:

{schema_description}

Instructions:
- Only use columns listed in the schema above. Never guess column names.
- Use only relevant columns for the specific question.
- **Do not use `auto_audit` or `false_audit` unless specifically asked**. 
    - These columns are only relevant if the question explicitly asks about the audit type (auto, manual, or false audit).
    - For **TP/FP analysis**, use `Audit_status`:
      - TP: Audit_status = 1
      - FP: Audit_status = 0
    -For **Audit Type ** use 'auto_audit':
      -Auto audits : auto_audit =1
      -Manual audits : auto_audit =0 
      -False Audits : dalse_audit =1
- If the question asks for an **audit count by site**, only use the necessary columns (like `site_name` and relevant date columns).
- For date filtering, always refer to `video_date`, `Audited_On`, `Created_On` depending on the context (unless specified otherwise).
- To know whether the records are autited or not use IsAudited = 1 -> audited else if IsAudited = 0 not audited.
Date Filtering Instructions:
- For questions about TPs and FPs:
  - Always apply any date filter on the `Audited_on` column.
-For Audit count :
  -Always apply any date filter on the `Created_On` column.
- Date filters examples:
  - If the question says 'today' or 'now', use for ex like :`video_date LIKE CONCAT(CURDATE(), '%')`.
  - Never use 'between' keyword for filtering for given date rage (like from and to dates),follow below:
    - If the question specifies an exact date like '2024-04-01' or yesterday or today kinds, use for ex like : `video_date Like 'YYYY-MM-DD%'`.
    - If the question specifies a date range, use for example: `video_date >= 'YYYY-MM-DD' AND video_date < 'YYYY-MM-DD'`.
    - Never assume today's date unless the user explicitly mentions it.

Other Instructions:
- Use `site_name` for site-level filters, and `site_id` for ID-based filters.
- Never add deleted = 0 or any similar condition unless specifically asked.
- Strictly follow the provided schema — no assumptions.

Question:
{question}

Respond with only the SQL query.

"""
def generate_sql_from_llm_statics(question: str)->str:
    """Uses LLM to generate SQL based on user question and current schema."""
    global global_site_statics_df
    if global_site_statics_df is None or global_site_statics_df.empty:
        return "-- No data loaded."

    schema_description = generate_schema_description(global_site_statics_df)
    prompt_template = build_query_prompt_statics(schema_description, question)

    prompt = PromptTemplate(
        input_variables=["question", "schema_description"],
        template=prompt_template
    )
    llm = Ollama(model="llama3", temperature=0)
    chain = LLMChain(llm=llm, prompt=prompt)

    try:
        output = chain.run({"question": question, "schema_description": schema_description})
        # Extract SQL using regex
        sql = re.search(r"(?i)(SELECT\s.+?)(?=;|$)", output, re.DOTALL)
        return sql.group(1).strip() if sql else output.strip()
    except Exception as e:
        return f"-- Error generating SQL: {e}"

def generate_sql_from_llm(question: str) -> str:
    """Uses LLM to generate SQL based on user question and current schema."""
    global global_df
    if global_df is None or global_df.empty:
        return "-- No data loaded."

    schema_description = generate_schema_description(global_df)
    prompt_template = build_query_prompt_audits(schema_description, question)

    prompt = PromptTemplate(
        input_variables=["question", "schema_description"],
        template=prompt_template
    )
    llm = Ollama(model="llama3", temperature=0)
    chain = LLMChain(llm=llm, prompt=prompt)

    try:
        output = chain.run({"question": question, "schema_description": schema_description})
        # Extract SQL using regex
        sql = re.search(r"(?i)(SELECT\s.+?)(?=;|$)", output, re.DOTALL)
        return sql.group(1).strip() if sql else output.strip()
    except Exception as e:
        return f"-- Error generating SQL: {e}"


def execute_sql_on_df(query: str,df : pd.DataFrame) -> pd.DataFrame:
    """Executes SQL query using pandasql on the global DataFrame."""
    if df is None or df.empty:
        return pd.DataFrame({"error": ["No data loaded to query."]})

    try:
        return ps.sqldf(query, {"df": df})
    except Exception as e:
        print(f"SQL execution error: {e}")
        return pd.DataFrame({"error": [str(e)]})

def execute_sql_on_df_statics(query : str, df : pd.DataFrame)->pd.DataFrame:
    """Executes SQL query using pandasql on the global DataFrame."""
    if df is None or df.empty:
        return pd.DataFrame({"error": ["No data loaded to query."]})
    try:
        return ps.sqldf(query, {"df": df})
    except Exception as e:
        print(f"SQL execution error: {e}")
        return pd.DataFrame({"error": [str(e)]})

