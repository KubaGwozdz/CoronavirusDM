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
    fig.update_layout(title_text="Tweets per day")
    return fig


def most_popular_hashtags_per_day(db):
    data = db.get_most_popular_hashtags_per_day()
    fig = go.Figure()
    for tag in data.keys():
        fig.add_trace(
            go.Scatter(
                x=data[tag]['dates'],
                y=data[tag]['numbers'],
                name=tag
            )
        )
    fig.update_layout(title_text="Most popular hashtags per day")
    return fig

def popular_users(db):
    data = db.get_most_popular_users()
    X = list(data.keys())
    Y = list(data.values())

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x = X,
        y = [y for (x,y) in Y],
        line_color ='deepskyblue',
        opacity=0.8
    ))
    fig.add_trace(go.Scatter(
        x = X,
        y = [x for (x,y) in Y],
        line_color = 'yellow',
        opacity=0.8
    ))
    return fig


if __name__ == "__main__":
    db = DataSelector()
    # tweets_per_day_fig = tweets_per_day(db)
    # tweets_per_day_fig.show()

    # tags_per_day_fig = most_popular_hashtags_per_day(db)
    # tags_per_day_fig.show()

    popular_users_fig = popular_users(db)
    popular_users_fig.show()