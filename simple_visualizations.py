from datetime import datetime

from data_selection import DataSelector
import plotly.graph_objects as go


def tweets_per_day(db):
    data = db.get_number_of_tweets_per_day()
    X = [datetime.strptime(d, "%Y-%m-%d %H:%M:%S").date() for d in data.keys()]
    Y = list(data.values())

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=X,
        y=Y,
        line_color='deepskyblue',
        opacity=0.8))
    return fig


if __name__ == "__main__":
    db = DataSelector()
    tweets_per_day_fig = tweets_per_day(db)
    tweets_per_day_fig.show()