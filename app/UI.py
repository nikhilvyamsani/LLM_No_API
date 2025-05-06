import streamlit as st
import plotly.express as px
import pandas as pd
from datetime import datetime,timedelta
import Main as backend
from apscheduler.schedulers.background import BackgroundScheduler

st.set_page_config(page_title="Anomaly Audit & Site Statics Assistant", layout="wide")

# Initialize session state
if "org" not in st.session_state:
    st.session_state["org"] = "sr_lnt"
    st.session_state["data_loaded"] = False
    st.session_state["results"] = None
    st.session_state["mode"] = "Anomaly Audit Assistant"
    st.session_state["insights"] = None
# Sidebar: Mode selection and Organization selection
with st.sidebar:
    st.header("🔧 Settings")
    mode = st.radio("Select Mode", ["Anomaly Audit Assistant", "Site Statics"], key="mode_radio")
    # Check if the organization or mode has changed
    if  mode != st.session_state["mode"]:
        st.session_state["mode"] = mode
        st.session_state["data_loaded"] = False  # Reset data loaded status
        st.session_state["results"] = None  # Reset results


# Anomaly Audit Assistant Mode
if mode == "Anomaly Audit Assistant":
    def on_org_change():
        # Reset data loaded status and results when the organization changes
        st.session_state["data_loaded"] = False
        st.session_state["results"] = None
        # Reload the data for the new organization
        selected_db = org_to_db[st.session_state["org_selectbox"]]
        st.session_state["org"] = selected_db
        load_data(selected_db)
    with st.sidebar:

        org = st.selectbox("Select Organization", [
            "Sekura", "Roadis-NH08", "Maple Highways", "Adani Road Transport Limited", "JHNSW", "Reliance"
        ], key="org_selectbox", on_change=on_org_change)
        org_to_db = {
            "Sekura": "sr_lnt",
            "Roadis-NH08": "nh08_roadis",
            "Maple Highways": "Maple_Highways",
            "Adani Road Transport Limited": "prs_tollways",
            "JHNSW": "JHNSW",
            "Reliance": "pstrpl"
        }
        selected_db = org_to_db[org]

        # Filters
        usernames = ["All"] + backend.get_all_usernames()
        selected_users = st.multiselect("Select user(s) (optional):", usernames)

        assets = ["All"] + backend.get_all_asset_names()
        selected_assets = st.multiselect("Select Asset(s) (optional):", assets)

        audit_types = ["All", "Auto Audits", "Manual Audits", "False audits"]
        selected_audit_types = st.multiselect("Select Audit Type(s) (optional):", audit_types)

        date_cols = ['video_date', 'Created_on', 'Audited_on', 'Deleted_on', 'updated_on',]
        selected_date = st.selectbox("Select Date Column", date_cols)

        st.session_state['selected_users'] = selected_users if selected_users else None
        st.session_state['selected_assets'] = selected_assets if selected_assets else None
        st.session_state['audit_types'] = selected_audit_types if selected_audit_types else None
        st.session_state['selected_date'] = selected_date if selected_date else None

        # Date range filter
        st.subheader("🗓️ Date Range")
        start_date = st.date_input("Start Date", datetime.today())
        end_date = st.date_input("End Date (excludes)", datetime.today()+timedelta(days=1))

        # FAQ Buttons
        st.subheader("📌 Quick FAQs")
        faq_col1, faq_col2 = st.columns(2)

        if faq_col1.button("Get Audit Count for each site"):
            question = f"Get the count of all the records which are not audited for each site,for {st.session_state['selected_date']} >= '{start_date}' and < '{end_date}'."
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
            question = f"Show TP and FP counts for each site, {st.session_state['selected_date']}  on >='{start_date}' and<'{end_date}'."
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

    st.title(f"📋 Anomaly Audit Dashboard - {org} ")

    # Load data (only once per session or org change)
    @st.cache_data(show_spinner=False)
    def load_data(selected_db):
        return backend.load_and_join_data(selected_db)

    # Use Streamlit spinner outside for visual feedback
    with st.spinner("Loading data..."):
        df = load_data(selected_db)

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

    # Define the on_org_change function

# Site Statics Mode
elif mode == "Site Statics":
    st.title(f"📋 Site Statics Dashboard ")

    org_to_db = {
        "Sekura": "sr_lnt",
        "Roadis-NH08": "nh08_roadis",
        "Maple Highways": "Maple_Highways",
        "Adani Road Transport Limited": "prs_tollways",
        "JHNSW": "JHNSW",
        "Reliance": "pstrpl"
    }

    # Function to load data from the backend
    def load_data_statics(org_to_db):
        # Load data from the backend
        return backend.load_all_site_statics(org_to_db)

    # Function to generate insights
    def generate_insights(df_statics):
        today = datetime.today()
        start_date = today - timedelta(days=2)
        end_date = today

        df_last_two_days = df_statics[(df_statics['video_date'] >= start_date.strftime('%Y-%m-%d')) & 
                                      (df_statics['video_date'] <= end_date.strftime('%Y-%m-%d'))]
        df_last_two_days['is_fully_processed'] = ((df_last_two_days['is_processed'] == 1) & (df_last_two_days['progress_value'] == 100))

        insights = df_last_two_days.groupby(['org_name', 'site_name']).agg(
            total_videos=('video_name', 'count'),
            processed_videos=('is_fully_processed','sum'),
            kms_covered=('total_distance_covered', 'sum')
        ).reset_index()

        return insights

    # Function to display insights
    def display_insights(insights):
        for org_name in insights['org_name'].unique():
            st.subheader(f"Insights for {org_name}")
            org_insights = insights[insights['org_name'] == org_name]

            total_videos = org_insights['total_videos'].sum()
            processed_videos = org_insights['processed_videos'].sum()
            not_processed_videos = total_videos - processed_videos

            fig_video_distribution = px.pie(
                names=['Processed', 'Not Processed'],
                values=[processed_videos, not_processed_videos],
                title=f'Video Distribution for {org_name}',
                hole=0.3,
                color_discrete_sequence=['#636EFA', '#FECB52']  # Different colors for processed and not processed
            )
            st.plotly_chart(fig_video_distribution)

            st.write("### Detailed Insights by Site")
            st.table(org_insights)

    # Function to run insights generation every 15 minutes
    def run_insights():
        with st.spinner("Loading data..."):
            df_statics = load_data_statics(org_to_db)
            if df_statics is not None and not df_statics.empty:
                insights = generate_insights(df_statics)
                st.session_state["insights"] = insights
            else:
                st.session_state["insights"] = None

    # Initialize scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_insights, 'interval', minutes=15)
    scheduler.start()

    # Run insights on initial load
    if "insights" not in st.session_state or st.session_state["insights"] is None:
        run_insights()

    # Button to show insights
    if st.button("View Insights"):
        if st.session_state["insights"] is not None:
            display_insights(st.session_state["insights"])
        else:
            st.write("No data loaded. Please wait for the data to be processed.")

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

# Ensure the scheduler is shut down properly when the app is closed
def shutdown_scheduler():
    scheduler.shutdown()

import atexit
atexit.register(shutdown_scheduler)