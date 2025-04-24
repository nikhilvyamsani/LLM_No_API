import streamlit as st
import Main as backend  
import datetime

st.set_page_config(page_title="LLM SQL Chat", layout="centered")

st.title("Ask Your Database")
st.markdown("Query your **MySQL** anomaly data using natural language")

# Quick FAQs
st.markdown("---")
st.subheader("\U0001F4CC Quick FAQs")

# Create columns for layout
col1, col2 = st.columns(2)

# Date picker button and input
with col1:
    with st.container():
        st.markdown("### \U0001F4C5 Select Date Range for Audit Count")
        col1a, col1b = st.columns(2)
        with col1a:
            from_date = st.date_input("Select start date (From):", min_value=datetime.date(2020, 1, 1))
        with col1b:
            to_date = st.date_input("Select end date (To):", min_value=datetime.date(2020, 1, 1))

    if st.button("get count by site"):
        st.session_state.faq_click = "audit_count"

if st.session_state.get("faq_click") == "audit_count":
    faq_question = f"Give me the count of audits that are created between {from_date} and {to_date} group by site name,Note : use only Created_On col for this"
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
    st.session_state.faq_click = None

st.markdown("<br>", unsafe_allow_html=True)

with col2:
    if st.button("TPs & FPs"):
        st.session_state.faq_click = "tp_fp"

if st.session_state.get("faq_click") == "tp_fp":
    faq_question = (
        "What is the total count of true positives (TP) and false positives (FP) in the anomaly audit records, "
        "that are audited today for each site"
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
    st.session_state.faq_click = None

# Initialize session state to store query results and SQL
if 'df' not in st.session_state:
    st.session_state.df = None
if 'sql' not in st.session_state:
    st.session_state.sql = None

# Natural language question input in a form
with st.form("natural_language_query_form"):
    user_question = st.text_input(
        "\U0001F5E3️ Ask anything about anomaly audits (e.g., show anomalies for SRTL site created today):"
    )
    submit_query = st.form_submit_button("Ask")

# Input validation
if submit_query:
    if not user_question.strip():
        st.warning("⚠️ Please ask a valid question before submitting.")
    else:
        with st.spinner("Thinking..."):
            try:
                sql, df = backend.ask_llm_and_execute(user_question)
                st.session_state.sql = sql
                st.session_state.df = df
                st.success("✅ Query executed successfully!")
            except Exception as e:
                st.error("❌ Error while processing your query:")
                st.code(str(e), language="bash")
                st.info("Try asking again in simpler language.")

# Schema browser with error handling
with st.expander("\U0001F4C4 View Joined Table Schema"):
    try:
        backend.create_joined_view()
        view_schema = backend.get_view_schema()
        st.markdown("### Joined View Schema")
        st.dataframe(
            [[col[0], col[1], col[2], col[3], col[4], col[5]] for col in view_schema],
            column_config={
                0: "Field",
                1: "Type",
                2: "Null",
                3: "Key",
                4: "Default",
                5: "Extra"
            },
            use_container_width=True
        )
        st.markdown("#### Table Content")
        st.info(
            f"This view combines data from both anomaly_audit and site tables. "
            f"It includes all fields from anomaly_audit plus an additional 'site_name' column "
            f"from the site table. You can query using either site_id or site_name."
        )
    except Exception as e:
        st.warning("⚠️ Unable to load schema.")
        st.code(str(e), language="bash")

# Show generated SQL query
if st.session_state.sql is not None:
    with st.expander("\U0001F50D View Generated SQL", expanded=True):
        st.code(st.session_state.sql, language="sql")

# Show query results with pagination if data is available
if st.session_state.df is not None and not st.session_state.df.empty:
    st.markdown("### \U0001F4CA Query Results")
    st.write(f"Found {len(st.session_state.df)} records")

    page_size = 100
    total_pages = len(st.session_state.df) // page_size + (1 if len(st.session_state.df) % page_size > 0 else 0)

    if total_pages > 0:
        col1, col2 = st.columns([1, 5])
        with col1:
            page = st.number_input("Page", min_value=1, max_value=total_pages, value=1)
        with col2:
            st.markdown(f"Page {page} of {total_pages}")

        start = (page - 1) * page_size
        end = start + page_size
        paginated_df = st.session_state.df.iloc[start:end]
        st.dataframe(paginated_df, use_container_width=True)
    else:
        st.info("No data available to display.")
elif st.session_state.df is not None:
    st.warning("Query returned no results.")

# Add a footer with helpful examples
st.markdown("---")
with st.expander("\U0001F4DD Example Queries"):
    st.markdown("""
    - Show me all anomalies from SRTL site  
    - Show anomalies created last week  
    - Count of audits done today  
    - TP and FP breakdown by site
    """)
