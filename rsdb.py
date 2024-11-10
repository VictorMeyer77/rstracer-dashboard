import os

import streamlit as st
from streamlit.logger import get_logger

LOGGER = get_logger(__name__)


def run():
    st.set_page_config(
        page_title="RSDB",
        page_icon="🛰️",
    )

    st.write("# Explore your rstracer database ! 🛰️")

    db_format = st.selectbox(
        "What's the format of your rstracer database ?",
        ["duckdb", "parquet", "csv"],
    )

    db_path = st.text_input("Database path", value=os.getenv("RSBD_PATH"))

    if st.button("Load 🚀"):
        os.environ["RSBD_FORMAT"] = db_format
        os.environ["RSBD_PATH"] = db_path


if __name__ == "__main__":
    run()
