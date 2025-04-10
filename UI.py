import streamlit as st
import Main as backend  # ✅ Adjusted to match your actual filename (not Main)

st.set_page_config(page_title="LLM SQL Chat", layout="centered")

st.title("Ask Your Database (LLaMA3 + MySQL)")
st.markdown("Query your **MySQL** database using natural language powered by LLaMA3.")

# 💬 User query input
user_question = st.text_input("Ask something (e.g., number of records, list users):")

if user_question:
    with st.spinner("Thinking..."):
        try:
            sql, df = backend.ask_llm_and_execute(user_question)
            st.success("✅ Query executed successfully!")
            st.code(sql, language="sql")
            st.dataframe(df)
        except Exception as e:
            st.error("❌ Error while processing your query:")
            st.code(str(e), language="bash")
            st.info("Try asking again in simpler language.")

# 📄 Optional DB Schema Preview
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
