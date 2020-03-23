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
    fig.update_layout(
        title_text="Tweets per day",
        xaxis_title="date",
        yaxis_title="number of tweets"
    )
    return fig


def most_popular_hashtags_per_day(db):
    data = db.get_most_popular_hashtags_per_day()
    fig = go.Figure()
    for tag in data.keys():
        fig.add_trace(
            go.Scatter(
                x=data[tag]['dates'],
                y=data[tag]['numbers'],
                name=tag,
                opacity=0.8
            )
        )
    fig.update_layout(
        title_text="Most popular hashtags per day",
        xaxis_title="date",
        yaxis_title="number of tweets with hashtag"
    )
    return fig


def popular_users(db):
    data = db.get_most_popular_users()
    X = list(data.keys())
    Y = list(data.values())

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=X,
        y=[friends for (followers, friends) in Y],
        name='friends'
    ))
    fig.add_trace(go.Bar(
        x=X,
        y=[followers for (followers, friends) in Y],
        name='followers'
    ))
    fig.update_layout(
        title_text='Most popular users',
        xaxis_title="username",
        yaxis_title="number of people"
    )
    return fig


def most_active_users(db):
    data = db.get_most_active_users()

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=[name for name in data.keys()],
            y=[number for number in list(data.values())]
        )
    )
    fig.update_layout(
        title_text='Most active users since 2020-03-07',
        xaxis_title="username",
        yaxis_title="number of tweets"
    )
    return fig


def most_active_users_per_day(db):
    data = db.get_most_active_users_per_day()
    fig = go.Figure()
    for tag in data.keys():
        fig.add_trace(
            go.Scatter(
                x=data[tag]['dates'],
                y=data[tag]['numbers'],
                name=tag,
                opacity=0.8
            )
        )
    fig.update_layout(
        title_text="Most active users per day",
        xaxis_title="date",
        yaxis_title="number of tweets"
    )
    return fig


if __name__ == "__main__":
    db = DataSelector()
    # tweets_per_day_fig = tweets_per_day(db)
    # tweets_per_day_fig.show()
    #
    # tags_per_day_fig = most_popular_hashtags_per_day(db)
    # tags_per_day_fig.show()
    #
    # popular_users_fig = popular_users(db)
    # popular_users_fig.show()
    #
    # active_users_fig = most_active_users(db)
    # active_users_fig.show()

    active_users_per_day_fig = most_active_users_per_day(db)
    active_users_per_day_fig.show()
