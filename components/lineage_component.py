import streamlit as st
import graphviz
import time
import os


def render_lineage_graph(app_instance):
    """Renders the data lineage graph in the Streamlit sidebar."""
    st.sidebar.markdown("---")
    st.sidebar.subheader("Data Lineage Flow")

    lineage_history = app_instance.lineage_history  # Access from app_instance

    if not lineage_history:
        st.sidebar.info("No lineage steps recorded yet. Upload a local file to start!")
        return

    dot = graphviz.Digraph(comment='Data Pipeline Lineage', graph_attr={'rankdir': 'TB'})

    for i, step in enumerate(lineage_history):
        label = f"Step {i + 1}: {step['name']}"
        if 'details' in step:
            detail_str = ', '.join([f"{k}: {v}" for k, v in step['details'].items() if v is not None and v != 'None'])
            if detail_str:
                label += f"\n({detail_str})"

        current_page_name = app_instance.page  # Access from app_instance
        page_name_for_highlight = ""
        if current_page_name == "Select Data Source":
            page_name_for_highlight = "Data Loaded"
        elif current_page_name == "Data Cleaning":
            page_name_for_highlight = "Cleaning Options Set"
        elif current_page_name == "Data Transformation":
            page_name_for_highlight = "Transformation Options Set"
        elif current_page_name == "Load Data":
            page_name_for_highlight = "Pipeline Created"
        elif current_page_name in ["Pipeline Success", "Pipeline Final"]:
            page_name_for_highlight = "Pipeline Created"

        if step['name'].startswith(page_name_for_highlight) or (current_page_name == "Login" and i == 0):
            dot.node(str(step['id']), label, style='filled', fillcolor='lightblue')
        else:
            dot.node(str(step['id']), label)

    for i in range(1, len(lineage_history)):
        prev_step_id = lineage_history[i - 1]['id']
        current_step_id = lineage_history[i]['id']
        dot.edge(str(prev_step_id), str(current_step_id))

    try:
        st.sidebar.graphviz_chart(dot)
    except Exception as e:
        st.sidebar.error("Failed to render lineage graph!")
        st.sidebar.exception(e)

        st.sidebar.markdown("---")
        st.sidebar.subheader("Graphviz Diagnostics (on error):")
        try:
            found_dot_paths = graphviz.dot.find_graphviz()
            st.sidebar.warning(f"graphviz.dot.find_graphviz() returned: `{found_dot_paths}`")
            if not found_dot_paths:
                st.sidebar.info(
                    "This means the Python `graphviz` library could not find the `dot` executable at runtime.")
                st.sidebar.info(
                    "Please ensure Graphviz is correctly installed and accessible in your system's PATH. Even if `which dot` works, Streamlit's environment might be different.")
                st.sidebar.info(f"Current `os.environ['PATH']` (from Streamlit): `{os.environ.get('PATH')}`")
                if os.environ.get("GRAPHVIZ_DOT"):
                    st.sidebar.info(f"Explicit `GRAPHVIZ_DOT` set to: `{os.environ.get('GRAPHVIZ_DOT')}`")
                else:
                    st.sidebar.info("`GRAPHVIZ_DOT` environment variable is NOT explicitly set.")
        except Exception as inner_e:
            st.sidebar.error(f"Error during `find_graphviz()` diagnostic: {inner_e}")
    st.sidebar.markdown("---")


def add_lineage_step(app_instance, name, details=None):
    """
    Adds a new step to the lineage history in the app_instance.

    Args:
        app_instance: The PipelineApp instance.
        name (str): A descriptive name for the step.
        details (dict, optional): Additional details about the step.
    """
    new_step = {
        "id": time.time(),
        "name": name,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "details": details if details is not None else {}
    }
    app_instance.lineage_history.append(new_step)
    # No st.rerun() here;
    # page navigation buttons will trigger reruns.
