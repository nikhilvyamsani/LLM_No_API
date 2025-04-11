import streamlit as st
import dummy_ as backend  # Adjusted to match your actual filename (not Main)

st.set_page_config(page_title="LLM SQL Chat", layout="centered")

st.title("Ask Your Database")
st.markdown("Query your **MySQL** database using natural language")

# User query input
user_question = st.text_input("Ask something (e.g., number of records, list users):")

# Initialize session state variables
if 'df' not in st.session_state:
    st.session_state.df = None
if 'sql' not in st.session_state:
    st.session_state.sql = None

if user_question:
    with st.spinner("Thinking..."):
        try:
            sql, df = backend.ask_llm_and_execute(user_question)
            st.session_state.sql = sql
            st.session_state.df = df
            st.success("✅ Query executed successfully!")
            st.code(st.session_state.sql, language="sql")
        except Exception as e:
            st.error("❌ Error while processing your query:")
            st.code(str(e), language="bash")
            st.info("Try asking again in simpler language.")

if st.session_state.df is not None:
    # Paginate data
    page_size = 100  # Number of rows per page
    total_pages = len(st.session_state.df) // page_size + (1 if len(st.session_state.df) % page_size > 0 else 0)
    page = st.number_input("Page", min_value=1, max_value=total_pages, value=1)
    start = (page - 1) * page_size
    end = start + page_size
    paginated_df = st.session_state.df.iloc[start:end]

    st.dataframe(paginated_df)

# Optional DB Schema Preview
with st.expander("📄 View DB Schema"):
    try:
        tables = backend.list_tables()
        for t in tables:
            st.subheader(f"🗂️ Table: {t}")
            # Use SHOW COLUMNS for MySQL
            conn, _ = backend.get_fresh_db()
            cursor = conn.cursor()
            cursor.execute(f"SHOW COLUMNS FROM {t}")
            rows = cursor.fetchall()
            col_df = st.data_editor(
                [{"Field": r[0], "Type": r[1], "Null": r[2], "Key": r[3], "Default": r[4], "Extra": r[5]} for r in rows],
                use_container_width=True,
                hide_index=True
            )
            cursor.close()
            conn.close()
    except Exception as e:
        st.warning("⚠️ Unable to load schema.")
        st.code(str(e), language="bash")