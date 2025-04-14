import streamlit as st
import Main as backend  # Replace 'Main' with your actual backend module if different

st.set_page_config(page_title="LLM SQL Chat", layout="centered")

st.title("Ask Your Database")
st.markdown("Query your **MySQL** database using natural language")

# Initialize session state variables early
if 'df' not in st.session_state:
    st.session_state.df = None
if 'sql' not in st.session_state:
    st.session_state.sql = None

# User query input
user_question = st.text_input("Ask something (e.g., number of records, list users):")

# Quick FAQs
st.markdown("---")
st.subheader("📌 Quick FAQs")

col1, col2 = st.columns(2)

with col1:
    if st.button("Audit count by site"):
        faq_question = "Give me the count of audits done today for each site / group by site Id,consider IsAudited for checking if done or not"
        with st.spinner("Fetching audit counts..."):
            try:
                sql, df = backend.ask_llm_and_execute(faq_question)
                st.session_state.sql = sql
                st.session_state.df = df
                st.success("✅ Query executed successfully!")
                st.code(sql, language="sql")
            except Exception as e:
                st.error("❌ Failed to fetch audit count:")
                st.code(str(e), language="bash")

with col2:
    if st.button("TPs & FPs"):
        faq_question = (
            "Give me total count of false positive and true positive anomaly audits for today group by siteId . consider Audited_On for filtering "
            "Remember: audit status is 1 for TP and 0 for FP and see the schema before generating query"
        )
        with st.spinner("Fetching TPs and FPs..."):
            try:
                sql, df = backend.ask_llm_and_execute(faq_question)
                st.session_state.sql = sql
                st.session_state.df = df
                st.success("✅ Query executed successfully!")
                st.code(sql, language="sql")
            except Exception as e:
                st.error("❌ Failed to fetch TP/FP counts:")
                st.code(str(e), language="bash")

# Handle user-input query
if user_question:
    with st.spinner("Thinking..."):
        try:
            sql, df = backend.ask_llm_and_execute(user_question)
            st.session_state.sql = sql
            st.session_state.df = df
            st.success("✅ Query executed successfully!")
            st.code(sql, language="sql")
        except Exception as e:
            st.error("❌ Error while processing your query:")
            st.code(str(e), language="bash")
            st.info("Try asking again in simpler language.")

# Display results with pagination
if st.session_state.df is not None and not st.session_state.df.empty:
    st.markdown("### 📊 Query Results")
    page_size = 100
    total_pages = len(st.session_state.df) // page_size + (1 if len(st.session_state.df) % page_size > 0 else 0)

    if total_pages > 0:
        page = st.number_input("Page", min_value=1, max_value=total_pages, value=1)
        start = (page - 1) * page_size
        end = start + page_size
        paginated_df = st.session_state.df.iloc[start:end]
        st.dataframe(paginated_df)
    else:
        st.info("No data available to display.")
elif st.session_state.df is not None:
    st.warning("Query returned no results.")

# Optional DB Schema Preview
with st.expander("📄 View DB Schema"):
    try:
        tables = backend.list_tables()
        for t in tables:
            st.subheader(f"🗂️ Table: {t}")
            conn, _ = backend.get_fresh_db()
            cursor = conn.cursor()
            cursor.execute(f"SHOW COLUMNS FROM {t}")
            rows = cursor.fetchall()
            st.data_editor(
                [{"Field": r[0], "Type": r[1], "Null": r[2], "Key": r[3], "Default": r[4], "Extra": r[5]} for r in rows],
                use_container_width=True,
                hide_index=True
            )
            cursor.close()
            conn.close()
    except Exception as e:
        st.warning("⚠️ Unable to load schema.")
        st.code(str(e), language="bash")
