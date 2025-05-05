import streamlit as st
import pandas as pd
from datetime import datetime
import dummy_ as backend

st.set_page_config(page_title="Anomaly Audit & Site Statics Assistant", layout="wide")

# Initialize session state
if "org" not in st.session_state:
    st.session_state["org"] = "sr_lnt"
    st.session_state["data_loaded"] = False
    st.session_state["results"] = None
    st.session_state["mode"] = "Anomaly Audit Assistant"

# Sidebar: Mode selection and Organization selection
with st.sidebar:
    st.header("🔧 Settings")
    mode = st.radio("Select Mode", ["Anomaly Audit Assistant", "Site Statics"], key="mode_radio")

    # Organization selection
    org = st.selectbox("Select Organization", [
        "Sekura", "Roadis-NH08", "Maple Highways", "Adani Road Transport Limited", "JHNSW", "Reliance"
    ])
    org_to_db = {
        "Sekura": "sr_lnt",
        "Roadis-NH08": "nh08_roadis",
        "Maple Highways": "Maple_Highways",
        "Adani Road Transport Limited": "prs_tollways",
        "JHNSW": "JHNSW",
        "Reliance": "pstrpl"
    }
    selected_db = org_to_db[org]

    # Check if the organization or mode has changed
    if selected_db != st.session_state["org"] or mode != st.session_state["mode"]:
        st.session_state["org"] = selected_db
        st.session_state["mode"] = mode
        st.session_state["data_loaded"] = False  # Reset data loaded status
        st.session_state["results"] = None  # Reset results

# Anomaly Audit Assistant Mode
if mode == "Anomaly Audit Assistant":
    st.title(f"📋 Anomaly Audit Dashboard - {org}")

    # Load data (only once per session or org change)
    @st.cache_data(show_spinner="Loading data...")
    def load_data(selected_db):
        st.write("Loading data...")
        return backend.load_and_join_data(selected_db)

    if not st.session_state["data_loaded"]:
        df = load_data(st.session_state["org"])
        if df is not None and not df.empty:
            st.session_state["data_loaded"] = True
            st.write("Data loaded successfully.")
        else:
            st.write("No data loaded.")
    else:
        st.write("Data loaded successfully.")

    with st.sidebar:
        # Filters
        usernames = ["All"] + backend.get_all_usernames()
        selected_users = st.multiselect("Select user(s) (optional):", usernames)

        assets = ["All"] + backend.get_all_asset_names()
        selected_assets = st.multiselect("Select Asset(s) (optional):", assets)

        audit_types = ["All", "Auto Audits", "Manual Audits", "False audits"]
        selected_audit_types = st.multiselect("Select Audit Type(s) (optional):", audit_types)

        st.session_state['selected_users'] = selected_users if selected_users else None
        st.session_state['selected_assets'] = selected_assets if selected_assets else None
        st.session_state['audit_types'] = selected_audit_types if selected_audit_types else None

        # Date range filter
        st.subheader("🗓️ Date Range")
        start_date = st.date_input("Start Date", datetime.today())
        end_date = st.date_input("End Date (excludes)", datetime.today())

        # FAQ Buttons
        st.subheader("📌 Quick FAQs")
        faq_col1, faq_col2 = st.columns(2)

        if faq_col1.button("Get Audit Count for each site"):
            question = f"Get the count of all the audits for each site, created >= '{start_date}' and < '{end_date}'."
            if st.session_state['audit_types']:
                question += f" Filter by audit types: {', '.join(st.session_state['audit_types'])}."
            if st.session_state['selected_users']:
                question += f" For users: {', '.join(st.session_state['selected_users'])}."
            if st.session_state['selected_assets']:
                question += f" Only include rows where asset is in ({', '.join(st.session_state['selected_assets'])})."
            st.session_state['question_passed'] = question
            sql = backend.generate_sql_from_llm(question)
            st.session_state['generated_sql'] = sql
            result_df = backend.execute_sql_on_df(sql)
            st.session_state['results'] = result_df

        if faq_col2.button("TP/FP Analysis"):
            question = f"Show TP and FP counts for each site, video_date >='{start_date}' and<'{end_date}'."
            if st.session_state['audit_types']:
                question += f" Filter by audit types: {', '.join(st.session_state['audit_types'])}."
            if st.session_state['selected_users']:
                question += f" For users: {', '.join(st.session_state['selected_users'])}."
            if st.session_state['selected_assets']:
                question += f" Only include rows where asset is in ({', '.join(st.session_state['selected_assets'])})."
            st.session_state['question_passed'] = question
            sql = backend.generate_sql_from_llm(question)
            st.session_state['generated_sql'] = sql
            result_df = backend.execute_sql_on_df(sql)
            st.session_state['results'] = result_df

    # Ask Anything Section
    st.markdown("### Ask Anything about the audits")

    custom_query = st.text_input("Ask anything about anomaly audits:")
    if st.button("Run Query"):
        st.session_state['question_passed'] = custom_query
        sql = backend.generate_sql_from_llm(custom_query)
        st.session_state['generated_sql'] = sql
        result_df = backend.execute_sql_on_df(sql)
        st.session_state['results'] = result_df

    # Display results section
    if st.session_state.get('results') is not None:
        st.markdown("#### 📊 Results")

        if 'question_passed' in st.session_state:
            st.write(f"**Question asked:** {st.session_state['question_passed']}")
        if 'generated_sql' in st.session_state:
            st.write(f"**Generated SQL:** {st.session_state['generated_sql']}")
        df = st.session_state['results']
        total_rows = len(df)
        page_size = 100
        total_pages = (total_rows - 1) // page_size + 1

        # Ensure total_pages is at least 1
        total_pages = max(total_pages, 1)

        # Page selector
        page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)

        # Slice the DataFrame for current page
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        st.dataframe(df.iloc[start_idx:end_idx])

        # Optional: show page info
        st.caption(f"Showing records {start_idx+1} to {min(end_idx, total_rows)} of {total_rows}")

# Site Statics Mode
elif mode == "Site Statics":
    st.title(f"📋 Site Statics Dashboard - {org}")

    # Load data (only once per session or org change)
    @st.cache_data(show_spinner="Loading data...")
    def load_data_statics(selected_db):
        st.write("Loading data...")  
        return backend.load_site_statics_data(selected_db)

    if not st.session_state["data_loaded"]:
        df_statics = load_data_statics(st.session_state["org"])
        if df_statics is not None and not df_statics.empty:
            st.session_state["data_loaded"] = True
            st.write("Data loaded successfully.")
        else:
            st.write("No data loaded.")
    else:
        st.write("Data loaded successfully.")

    # Ask Anything Section
    st.markdown("### Ask Anything about the site statics")
    custom_query_statics = st.text_input("Ask anything about site statics:")
    if st.button("Run Query"):
        st.session_state['question_passed'] = custom_query_statics
        sql = backend.generate_sql_from_llm_statics(custom_query_statics)
        st.session_state['generated_sql'] = sql
        result_df_statics = backend.execute_sql_on_df_statics(sql)
        st.session_state['results'] = result_df_statics

    # Display results section
    if st.session_state.get('results') is not None:
        st.markdown("#### 📊 Results")

        if 'question_passed' in st.session_state:
            st.write(f"**Question asked:** {st.session_state['question_passed']}")
        if 'generated_sql' in st.session_state:
            st.write(f"**Generated SQL:** {st.session_state['generated_sql']}")

        df_statics = st.session_state['results']
        total_rows = len(df_statics)
        page_size = 100
        total_pages = (total_rows - 1) // page_size + 1

        # Ensure total_pages is at least 1
        total_pages = max(total_pages, 1)

        # Page selector
        page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)

        # Slice the DataFrame for current page
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        st.dataframe(df_statics.iloc[start_idx:end_idx])

        # Optional: show page info
        st.caption(f"Showing records {start_idx+1} to {min(end_idx, total_rows)} of {total_rows}")