import os
from time import sleep

import streamlit as st
from streamlit.logger import get_logger

from rstracer import Rstracer

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

    load_column = st.columns(2)
    with load_column[0]:
        if st.button("Load 🚀"):
            os.environ["RSBD_FORMAT"] = db_format
            os.environ["RSBD_PATH"] = db_path
            Rstracer().stop()
            with load_column[1]:
                progress_bar = st.progress(0, text="Loading...")
                for percent_complete in range(100):
                    sleep(0.01)
                    progress_bar.progress(percent_complete, text="Loading...")
                progress_bar.progress(100, text="Ready !")

    st.divider()
    live_column = st.columns(2)
    with live_column[0]:
        if st.button("Live Mode 🚀"):
            st.sidebar.warning(
                "Warning: This program requires sudo permissions. Please check your console to enter your password."
            )
            Rstracer().launch()
            os.environ["RSBD_FORMAT"] = "parquet"
            os.environ["RSBD_PATH"] = ".output/rstracer"
            with live_column[1]:
                progress_bar = st.progress(0, text="Initializing...")
                for percent_complete in range(100):
                    sleep(0.07)
                    progress_bar.progress(percent_complete, text="Initializing...")
            progress_bar.progress(100, text="Ready !")


if __name__ == "__main__":
    run()
