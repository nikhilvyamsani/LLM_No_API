import streamlit as st
import pandas as pd
from datetime import datetime
import Main as backend

st.set_page_config(page_title="Anomaly Audit Assistant", layout="wide")

# Session state for selected org/db
if "org" not in st.session_state:
    st.session_state["org"] = "sr_lnt"
    st.session_state["data_loaded"] = False
    st.session_state["results"] = None

# Sidebar: Organization selection and filters
with st.sidebar:
    st.header("🔧 Settings")

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

    # Check if the organization has changed
    if selected_db != st.session_state["org"]:
        st.session_state["org"] = selected_db
        st.session_state["data_loaded"] = False  # Reset data loaded status
        st.session_state["results"] = None  # Reset results

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
    end_date = st.date_input("End Date", datetime.today())

    # FAQ Buttons
    st.subheader("📌 Quick FAQs")
    faq_col1, faq_col2 = st.columns(2)

    if faq_col1.button("Get Audit Count for each site"):
        question = f"Get the count of all the audits for each site, created between '{start_date}' and '{end_date}'."

        if st.session_state['audit_types']:
            audit_list = st.session_state['audit_types']
            question += " Filter by audit types: " + ", ".join(st.session_state['audit_list']) + "."

        if st.session_state['selected_users']:
            user_list = "', '".join(st.session_state['selected_users'])
            question += " For users: " + ", ".join(st.session_state['selected_users']) + "."

        if st.session_state['selected_assets']:
            asset_list = "', '".join(st.session_state['selected_assets'])
            question += f" Only include rows where asset is in ('{asset_list}')."

        st.session_state['question_passed'] = question
        sql = backend.generate_sql_from_llm(question)
        st.session_state['generated_sql'] = sql
        result_df = backend.execute_sql_on_df(sql)
        st.session_state['results'] = result_df

    if faq_col2.button("TP/FP Analysis"):
        question = f"Show TP and FP counts for each site, video_date between '{start_date}' and '{end_date}'."

        if st.session_state['audit_types']:
            audit_list = st.session_state['audit_types']
            question += " Filter by audit types: " + ", ".join(st.session_state['audit_list']) + "."

        if st.session_state['selected_users']:
            user_list = "', '".join(st.session_state['selected_users'])
            question += " For users: " + ", ".join(st.session_state['selected_users']) + "."

        if st.session_state['selected_assets']:
            asset_list = "', '".join(st.session_state['selected_assets'])
            question += f" Only include rows where asset is in ('{asset_list}')."

        st.session_state['question_passed'] = question
        sql = backend.generate_sql_from_llm(question)
        st.session_state['generated_sql'] = sql
        result_df = backend.execute_sql_on_df(sql)
        st.session_state['results'] = result_df

# Main area for displaying results
st.title(f"📋 Audit Dashboard - {org}")

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

# Ask Anything Section
st.markdown("### Ask Anything about the audits")

custom_query = st.text_input("Ask anything about anomaly audits:")
if st.button("Run Query") and custom_query:
    st.session_state['question_passed'] = custom_query
    sql = backend.generate_sql_from_llm(custom_query)
    st.session_state['generated_sql'] = sql
    result_df = backend.execute_sql_on_df(sql)
    st.session_state['results'] = result_df  # Store the result in the session state

# Display results section - now displayed only once
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