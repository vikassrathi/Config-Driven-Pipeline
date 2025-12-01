import streamlit as st
import pandas as pd


def page_data_transformation(app_instance):
    """
    Placeholder page for Data Transformation, specifically for aggregation tasks.
    """
    st.title("Data Transformation")
    st.info("This page is not build yet.")

    if app_instance.current_df is not None and not app_instance.current_df.empty:
        st.write(f"Current DataFrame Shape: {app_instance.current_df.shape}")
        st.dataframe(app_instance.current_df.head())
    else:
        st.info("No data available.")


    col1, col2 = st.columns(2)
    with col1:
        if st.button("Back: Data Cleaning", key="back_from_transformation_to_cleaning"):
            app_instance.page = "Data Cleaning"
            st.rerun()
    with col2:
        if st.button("Next: Go to Load Data", key="next_from_transformation_to_load"):
            app_instance.page = "Load Data"
            st.rerun()
