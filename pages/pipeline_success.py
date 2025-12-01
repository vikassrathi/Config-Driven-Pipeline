import streamlit as st

def page_pipeline_success(app_instance):
    """Displays a success message after pipeline creation."""
    st.balloons()
    st.success(" Data Pipeline Created Successfully! (Mock) 🎉")
    if st.button("Okay", key="pipeline_success_ok"):
        app_instance.page = "Pipeline Final"
        st.rerun()
