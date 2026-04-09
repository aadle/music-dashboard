import polars as pl
import plotly.graph_objects as go
from data_setup import combine_data_sources


def contribution_graph(df: pl.DataFrame, year: int):
    colorscale_gh = ["#D4D3C7", "#ACEEBB", "#4AC36B", "#2EA44E", "#126429"]

    fig = go.Figure(
        data=go.Heatmap(
            z=df["tracks_scrobbled"],
            x=df["week"],
            y=df["weekday"],
            colorscale=colorscale_gh,
            showscale=True,
            xgap=3,
            ygap=3,
            hovertext=df["date"].dt.strftime("%B %d, %Y"),
            hovertemplate="<b>Date:</b> %{hovertext}<br><b>Count:</b> %{z}<extra></extra>",
        )
    )

    fig.update_layout(
        title=f"{year} Listening activity",
        height=250,
        width=2000,
        xaxis=dict(
            showgrid=False,
            zeroline=False,
            tickmode="array",
            # tickvals=[
            #     int(pd.Timestamp(2024, m, 1).strftime("%W"))
            #     for m in range(1, 13)
            # ],
            ticktext=[
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ],
        ),
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            autorange="reversed",
            scaleanchor="x",
            scaleratio=1,
        ),
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=40, b=40, l=40, r=40),
    )

    return fig


def main(df):
    year = 2025
    df_ = (
        df.filter(pl.col("track_played_utc").dt.year() == year)
        .group_by(pl.col("track_played_utc").dt.date())
        .agg(tracks_scrobbled=pl.len())
        .sort(pl.col("track_played_utc"), descending=False)
    )

    fig = go.Figure(
        data=go.Heatmap(
            z=df_["tracks_scrobbled"],
            # x=df_["track_played_utc"].dt.week(),
            x=df_["track_played_utc"].dt.strftime("%W").cast(int),
            y=df_["track_played_utc"].dt.weekday(),
            # colorscale=colorscale_gh,
            showscale=True,
            xgap=3,
            ygap=3,
            hovertext=df_["track_played_utc"].dt.strftime("%Y/%m/%d"),
            hovertemplate="<b>Date:</b> %{hovertext}<br><b>Count:</b> %{z}<extra></extra>",
        )
    )
    fig.show()

    print(df["track_played_utc"].dt.week())
    print(df["track_played_utc"].dt.weekday())


if __name__ == "__main__":
    df = combine_data_sources()
    main(df)
    pass
