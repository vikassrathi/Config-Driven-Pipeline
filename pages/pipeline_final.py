import streamlit as st


def page_pipeline_final(app_instance):
    """Allows the user to create a new pipeline or conclude."""
    st.title("Data Pipeline Completed")
    if st.button("Create New Pipeline", key="create_new_pipeline_button"):
        keys_to_clear = [key for key in st.session_state.keys() if key not in ["page", "jwt_token", "app_instance"]]
        for key in keys_to_clear:
            del st.session_state[key]

        app_instance.__init__()
        app_instance.page = "Select Data Source"  # Reset to the data selection page
        st.rerun()
