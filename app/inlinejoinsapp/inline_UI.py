import streamlit as st
import datetime
import dummy_inline as backend

st.set_page_config(page_title="LLM SQL Chat", layout="wide")
st.title("Ask Your Database")
st.markdown("Query your **MySQL** anomaly data using natural language")

# Initialize session state
for key in ['faq_click', 'selected_assets', 'audit_type_option', 'selected_users', 'df', 'sql', 'selected_org']:
    if key not in st.session_state:
        st.session_state[key] = None

# Layout: Two columns - left for filters and FAQs, right for results
col1, col2 = st.columns([3, 7])

with col1:
    # Left side - Filters and FAQs
    st.markdown("### 🏢 Select Organization")
    org_options = [
        "Sekura", "Roadis-NH08", "Maple Highways", "Adani Road Transport Limited", "JHNSW", "Reliance"
    ]
    selected_org = st.selectbox("Select Organization:", org_options)
    st.session_state.selected_org = selected_org

    # Update ANOMALY_DB based on selected organization
    org_db_mapping = {
        "Sekura": "sr_lnt",
        "Roadis-NH08": "nh08_roadis",
        "Maple Highways": "Maple_Highways",
        "Adani Road Transport Limited": "prs_tollways",
        "JHNSW": "JHNSW",
        "Reliance": "pstrpl"
    }

    if selected_org in org_db_mapping:
        anomaly_db = org_db_mapping[selected_org]
        st.session_state.anomaly_db = anomaly_db
        backend.set_anomaly_db(anomaly_db)

    # Global Audit Type Multiselect
    st.markdown("### 📋 Select Audit Type (optional):")
    audit_type_option = st.multiselect(
        "Select Audit Type(s):", ["All", "manual_audit", "auto_audit", "false_audit"]
    )
    st.session_state.audit_type_option = audit_type_option

    # Global Date Range Input
    st.markdown("### 📅 Select Date Range")
    col_date1, col_date2 = st.columns(2)
    with col_date1:
        from_date = st.date_input("Start Date:", min_value=datetime.date(2020, 1, 1))
    with col_date2:
        to_date = st.date_input("End Date:", min_value=datetime.date(2020, 1, 1))

    st.session_state.from_date = from_date
    st.session_state.to_date = to_date

    # Quick FAQs Section
    st.markdown("---")
    st.subheader("📌 Quick FAQs")
    with st.expander("FAQ: Audit Count by Site", expanded=True):
        asset_options = [
            "Hectometer_Stone", "Solar_Blinker", "Signboard_Information_Board", "Overspeeding", "Cracks",
            "Kilometer_Stone", "Street_Light_HNW", "High_Mast", "Street_Light_NW", "Street_Light",
            "Signboard_Hazard_Board", "Bus_Shelter", "Signboard_Caution_Board", "Bad_Kerbs", "Bad_Mbcb",
            "Sand", "Signboard_Mandatory_Board", "DB_box", "Encroachment", "VMS_Gantry", "ATCC",
            "Bad_Drainage", "Signboard_Gantry_Board", "Pot_Holes", "Earth_Works", "Hoardings",
            "Cattle_movement", "Water_Stagnation", "Vegetation_Growth", "Shop_Board", "Flex_Banner",
            "ECB_(SOS)", "Road_Markings", "Plants", "Delineator", "CCTV", "Signboard_Chevron_Board",
            "Pothole", "Low_Mast", "Wall_poster"
        ]
        selected_assets = st.multiselect("Select Asset(s):", asset_options)

        if st.button("Get Count by Site"):
            st.session_state.faq_click = "audit_count"
            st.session_state.selected_assets = selected_assets

    with st.expander("FAQ: TP/FP", expanded=True):
        user_names = [
            "Shubham", "mayavel", "ramu", "nadhiya", "navjoth", "Navjot", "Hari", "Murali",
            "Dhanush", "Mallikarjun", "Murli", "Bhargav", "Deepika", "Mallikarjuna", "Nathiya",
            "Pallavi", "Ramya", "Rishika","surendra"
        ]
        unique_user_names = sorted(set(user_names))

        selected_users = st.multiselect("Select Audited User:", unique_user_names)

        if st.button("TPs & FPs"):
            st.session_state.faq_click = "tp_fp"
            st.session_state.selected_users = selected_users

    # Natural Language Input
    with st.form("natural_language_query_form"):
        user_question = st.text_input("🗣️ Ask anything about anomaly audits:")
        submit_query = st.form_submit_button("Ask")

with col2:
    # Right side - Query Results

    # FAQ Execution: Audit Count
    if st.session_state.faq_click == "audit_count":
        asset_str = ", ".join(st.session_state.selected_assets) or "all assets"
        audit_type_clause = (
            f"Filter only for: {', '.join(st.session_state.audit_type_option)}"
        )

        faq_question = (
            f"Give me the count of all the audits that are created between {st.session_state.from_date} and {st.session_state.to_date} "
            f"for assets in {{{asset_str}}} grouped by site name. "
            f"do not use video_date column for filtering for this question"
            f"Note: use only Created_On column for given date range.{audit_type_clause},and grouped by site name."
        )

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

    # FAQ Execution: TP/FP
    if st.session_state.faq_click == "tp_fp":
        user_str = ", ".join(st.session_state.selected_users) or "all"

        # Build audit type clause only if user selected specific audit types
        audit_type_clause = ""
        if st.session_state.audit_type_option:
            audit_type_clause = f" Only consider {', '.join(st.session_state.audit_type_option)} audits."

        faq_question = (
            f"Provide the total count of true positives (TP) and false positives (FP) "
            f"audited by the users: {user_str} between {st.session_state.from_date} and {st.session_state.to_date}."
            f"{audit_type_clause} Group the results by site name."
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

    # Natural Language Query Execution
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

    # SQL Viewer
    if st.session_state.sql:
        with st.expander("🔍 View Generated SQL", expanded=True):
            st.code(st.session_state.sql, language="sql")

    # Paginated Results Viewer
    if st.session_state.df is not None:
        if not st.session_state.df.empty:
            st.markdown("### 📊 Query Results")
            st.write(f"Found {len(st.session_state.df)} records")
            st.dataframe(st.session_state.df, use_container_width=True)
        else:
            st.warning("Query returned no results.")

    # Example Queries
    st.markdown("---")
    with st.expander("📝 Example Queries"):
        st.markdown("""
        - Show me all anomalies from SRTL site  
        - Show anomalies created last week  
        - Count of audits done today  
        - TP and FP breakdown by site  
        """)
