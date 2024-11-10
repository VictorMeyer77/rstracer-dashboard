import streamlit as st
from streamlit.logger import get_logger

LOGGER = get_logger(__name__)


def run():
    st.set_page_config(
        page_title="RSDB",
        page_icon="👋",
    )

    st.write("# Explore your rstracer database ! 👋")

    st.sidebar.success("Select a demo above.")

    st.markdown(
        """
       TODO
    """
    )


if __name__ == "__main__":
    run()