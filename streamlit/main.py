import streamlit as st
import polars as pl
from data_utils import combine_data_sources

df = combine_data_sources()
# Lower case to as a measure to keep names consistent. Since the dataset is
# comprised by data from both Last.fm and Spotify, naming may vary between the
# platforms.
df = df.with_columns(
    artist_name = pl.col("artist_name").str.to_lowercase(),
    track_name = pl.col("track_name").str.to_lowercase(),
    album_name = pl.col("album_name").str.to_lowercase()
) 
st.session_state["scrobble_data"] = df

# Yearly analysis pages
analysis_2025 = st.Page("yearly analysis/analysis_2025.py", title="2025",
                        default=True)
yearly_overview = st.Page(
    "yearly analysis/yearly_overview.py",
    title="yearly overview",
)

# roadmap/todo page
todo = st.Page("todo/todo.py", title="To-do")

pg = st.navigation(
    {"Yearly analysis": [
        analysis_2025, 
        yearly_overview
    ],
     "Roadmap": [todo]
     }
)

pg.run()

if __name__ == "__main__":
    pass
