from datetime import datetime

from db_utils.data_selector import DataSelector
import plotly.graph_objects as go
import random
from visualizations.fig_utils import save_fig
import statistics as stat

def str_to_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()


callendar = {
    str_to_date('2020-03-11'): "WHO wprowadza stan pandemii",
    str_to_date('2020-03-12'): "Pierwsza ofiara w Polsce",
    str_to_date('2020-03-15'): "Zamknięcie granic oraz restauracji",
    str_to_date('2020-03-16'): "Zamknięcie barów w NY",
    str_to_date('2020-03-18'): "Darmowe testy w USA",
    str_to_date('2020-03-27'): "Boris Johnson zakażony",
    str_to_date('2020-03-30'): "Prezydent Duda zakłada konto na TikTok",
    str_to_date('2020-03-31'): "Zamknięcie parków",
    str_to_date('2020-04-05'): "Premier WB trafia do szpitala",
    str_to_date('2020-04-06'): "Premier WB zostaje przeniesionsy na OIOM",
    str_to_date('2020-04-09'): "Przeniesienie matur",
    str_to_date('2020-04-12'): "Boris Johnson opuszcza OIOM",
    str_to_date('2020-04-16'): "Nakaz noszenia maseczek",
    str_to_date('2020-04-20'): "Otwarcie parków",
    str_to_date('2020-04-24'): "Ogłoszenie terminów matur",
    str_to_date('2020-04-25'): "Premier Morawiecki ogłasza powrót ekstraklasy",
    str_to_date('2020-04-29'): "Po 4.05 mają zostać otwarte hotele",
}


def find_y_of(date, x, y):
    i = 0
    while i < len(x):
        if x[i] == date:
            return y[i]
        i += 1

    return y[-1]


def add_annotations(fig, x, y):
    for date, event in callendar.items():
        pos = find_y_of(date, x, y)
        fig.add_trace(go.Scatter(
            x=[date],
            y=[pos],
            name=event,
            marker_symbol="x",
            marker_size=10
        ))

    return fig


def tweets_per_day(db):
    data = db.get_number_of_tweets_per_day()
    X = [datetime.strptime(d, "%Y-%m-%d %H:%M:%S").date() for d in data.keys()]
    Y = list(data.values())

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=X,
        y=Y,
        line_color='deepskyblue',
        opacity=0.8,
        name="Tweets per day"
    ))
    fig.update_layout(
        title_text="Tweets per day",
        xaxis_title="date",
        yaxis_title="number of tweets"
    )
    return add_annotations(fig, X, Y)


def retweets_per_day(db):
    data = db.get_number_of_retweets_per_day()
    X = [datetime.strptime(d, "%Y-%m-%d %H:%M:%S").date() for d in data.keys()]
    Y = list(data.values())

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=X,
        y=Y,
        line_color='deepskyblue',
        opacity=0.8,
        name="Retweets per day"
    ))
    fig.update_layout(
        title_text="Retweets per day",
        xaxis_title="date",
        yaxis_title="number of retweets"
    )
    return add_annotations(fig, X, Y)


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


def most_popular_hashtags(db, from_date=None, to_date=None, month_name=None):
    data = db.get_most_popular_hashtags(from_date, to_date)
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=list(data.keys()),
            y=list(data.values()),
            name='hashtags',
            opacity=0.8
        )
    )
    fig.update_layout(
        title_text="Most popular hashtags" if month_name is None else "Most popular hashtags in " + month_name,
        xaxis_title="hashtag",
        yaxis_title="number of tweets with hashtag"
    )
    return fig


def popular_users(db):
    data_followers = db.get_most_popular_users_followers()
    data_friends = db.get_most_popular_users_friends()
    X_followers = list(data_followers.keys())
    Y_followers = list(data_followers.values())
    X_friends = list(data_friends.keys())
    Y_friends = list(data_friends.values())

    fig1 = go.Figure()
    fig2 = go.Figure()
    fig1.add_trace(go.Bar(
        x=X_friends,
        y=Y_friends,
        name='friends'
    ))
    fig2.add_trace(go.Bar(
        x=X_followers,
        y=Y_followers,
        name='followers'
    ))
    fig1.update_layout(
        title_text='Most popular users due to friends',
        xaxis_title="username",
        yaxis_title="number of people"
    )
    fig2.update_layout(
        title_text='Most popular users due to followers',
        xaxis_title="username",
        yaxis_title="number of people"
    )
    return fig1, fig2


def most_active_users(db, from_date=None, to_date=None, month_name=None):
    data = db.get_most_active_users(from_date, to_date)

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=[name for name in data.keys()],
            y=[number for number in list(data.values())]
        )
    )
    fig.update_layout(
        title_text='Most active users (tweets) since 2020-03-07' if month_name is None  else "Most active users (tweets) in "+month_name,
        xaxis_title="username",
        yaxis_title="number of tweets"
    )
    return fig


def most_active_users_retweets(db, from_date=None, to_date=None, month_name=None):
    data = db.get_most_active_users_retweets(from_date, to_date)

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=[name for name in data.keys()],
            y=[number for number in list(data.values())]
        )
    )
    fig.update_layout(
        title_text='Most active users (retweets) since 2020-03-07' if month_name is None  else "Most active users (retweets) in "+month_name,
        xaxis_title="username",
        yaxis_title="number of retweets"
    )
    return fig


def most_active_users_per_day_tweets(db):
    data = db.get_most_active_users_per_day_tweets()
    fig = go.Figure()
    for s_name in data.keys():
        fig.add_trace(
            go.Scatter(
                x=data[s_name]['dates'],
                y=data[s_name]['numbers'],
                name=s_name,
                opacity=0.8
            )
        )
    fig.update_layout(
        title_text="Most active tweeting users per day",
        xaxis_title="date",
        yaxis_title="number of tweets"
    )
    return fig


def newly_created_accounts(db):
    data = db.get_newly_created_accounts()

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=[date for date in data.keys()],
            y=[number for number in list(data.values())]
        )
    )
    fig.update_layout(
        title_text='Newly created accounts',
        xaxis_title="date",
        yaxis_title="number of new accounts"
    )
    return fig


def newly_created_accounts_pattern(db):
    data = db.get_newly_created_accounts_pattern()

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=[date for date in data.keys()],
            y=[number for number in list(data.values())]
        )
    )
    fig.update_layout(
        title_text='Newly created accounts where name matched pattern',
        xaxis_title="date",
        yaxis_title="number of new accounts"
    )
    return fig


def tw_lang_per_day(db):
    data = db.get_tw_lang_per_day()
    fig = go.Figure()
    for lang in data.keys():
        fig.add_trace(
            go.Scatter(
                x=data[lang]['dates'],
                y=data[lang]['numbers'],
                name=lang,
                opacity=0.8
            )
        )
    fig.update_layout(
        title_text="Tweets in language per day",
        xaxis_title="date",
        yaxis_title="number of tweets"
    )
    return fig


def influencers_per_day(db):
    data = db.get_influencers_per_day()
    t_fig = go.Figure()
    colors = ['#F39C12', '#E12FBD', '#9E3D64', '#9D22E3', '#F1C40F',
              '#05E5F8', '#E74C3C', '#5F287F', '#6593F5', '#4F820D',
              '#33F805', '#1AB394', '#154EE8', '#2D383C', '#B19CD9']
    i = 0
    for s_name in data.keys():
        t_fig.add_trace(
            go.Scatter(
                x=data[s_name]['dates'],
                y=data[s_name]['numbers'],
                name=s_name,
                line=dict(
                    color=colors[i]
                ),
                opacity=0.8
            )
        )
        i += 1
    t_fig.update_layout(
        title_text="Influencers' tweets per day",
        xaxis_title="date",
        yaxis_title="number of tweets",
    )
    rtw_fig = go.Figure()
    for s_name in data.keys():
        rtw_fig.add_trace(
            go.Scatter(
                x=data[s_name]['dates'],
                y=data[s_name]['retweet_count'],
                name=s_name,
                opacity=0.8
            )
        )
    rtw_fig.update_layout(
        title_text="Retweets of Influencers' tweets per day",
        xaxis_title="date",
        yaxis_title="number of retweets"
    )
    rep_fig = go.Figure()
    for s_name in data.keys():
        rep_fig.add_trace(
            go.Scatter(
                x=data[s_name]['dates'],
                y=data[s_name]['reply_count'],
                name=s_name,
                opacity=0.8
            )
        )
    rep_fig.update_layout(
        title_text="Replies to Influencers' tweets per day",
        xaxis_title="date",
        yaxis_title="number of replies"
    )
    q_fig = go.Figure()
    for s_name in data.keys():
        q_fig.add_trace(
            go.Scatter(
                x=data[s_name]['dates'],
                y=data[s_name]['quote_count'],
                name=s_name,
                opacity=0.8
            )
        )
    q_fig.update_layout(
        title_text="Quotes of Influencers' tweets per day",
        xaxis_title="date",
        yaxis_title="number of quotes"
    )

    return t_fig, rtw_fig, rep_fig, q_fig


def polish_sentiment(db):
    data = db.get_polish_tweets_sentiment()
    fig = go.Figure()
    data['color'] = [1 if e > 0 else 0 for e in data['sentiment']]
    fig.add_trace(go.Bar(
        x=data['date'],
        y=data['sentiment'],
        error_y=dict(
            type='data',
            array=data['stdev']
        ),
        marker={'color': data['sentiment'], 'colorscale': 'RdYlGn'}
    ))
    fig.update_layout(
        title_text="Sentiment of polish tweets",
        xaxis_title="date",
        yaxis_title="sentiment"
    )
    return fig


def polish_sentiment_with_daily_tweets(db):
    data = db.get_polish_tweets_sentiment()
    number = db.get_number_daily_polish_tweets()
    fig = go.Figure()
    data['color'] = [1 if e > 0 else 0 for e in data['sentiment']]
    fig.add_trace(go.Bar(
        x=data['date'],
        y=data['sentiment'],
        error_y=dict(
            type='data',
            array=data['stdev']
        ),
        marker={'color': data['sentiment'], 'colorscale': 'RdYlGn'},
        name="Sentiment"
    ))
    fig.add_trace(go.Scatter(
        x = number['date'],
        y = number['number'],
        name = 'Number of tweets'
    ))
    fig.update_layout(
        title_text="Sentiment of polish tweets",
        xaxis_title="date",
        yaxis_title="sentiment"
    )
    return fig


def sentiment_per_state_per_day(db):
    data = db.get_sentiment_per_state_per_day()
    fig = go.Figure()
    for state in data.keys():
        if state == 'us':
            fig.add_trace(go.Scatter(
                x=data[state]['dates'],
                y=data[state]['sentiment'],
                error_y=dict(type='data', array=data[state]['stdev']),
                name='AVERAGE',
                mode="lines",
                opacity=1,
                line=dict(width=2, color="red")
            ))
        else:
            fig.add_trace(go.Scatter(
                x=data[state]['dates'],
                y=data[state]['sentiment'],
                error_y=dict(type='data', array=data[state]['stdev']),
                name=data[state]['name'],
                mode="lines",
                opacity=0.5,
                line=dict(width=1)

            ))
    fig.update_layout(
        title_text="Sentiment in different states",
        xaxis_title="date",
        yaxis_title="Seniment"
    )
    return fig


if __name__ == "__main__":
    db = DataSelector()

    march = datetime.strptime("2020-03-01", "%Y-%m-%d").date(), datetime.strptime("2020-03-31", "%Y-%m-%d").date(), "march"
    april = datetime.strptime("2020-04-01", "%Y-%m-%d").date(), datetime.strptime("2020-04-30", "%Y-%m-%d").date(), "april"

    # tweets_per_day_fig = tweets_per_day(db)
    # tweets_per_day_fig.show()
    #
    # retweets_per_day_fig = retweets_per_day(db)
    # retweets_per_day_fig.show()

    # tags_per_day_fig = most_popular_hashtags_per_day(db)
    # tags_per_day_fig.show()
    #
    # popular_users_followers, popular_users_friends = popular_users(db)
    # popular_users_followers.show()
    # popular_users_friends.show()
    #
    # active_users_march_fig = most_active_users(db, *march)
    # active_users_march_fig.show()
    #
    # most_active_users_march_retweets_fig = most_active_users_retweets(db, *march)
    # most_active_users_march_retweets_fig.show()

    # most_popular_hashtags_march_fig = most_popular_hashtags(db, *march)
    # most_popular_hashtags_march_fig.show()
    #
    # active_users_april_fig = most_active_users(db, *april)
    # active_users_april_fig.show()
    #
    # most_active_users_april_retweets_fig = most_active_users_retweets(db, *april)
    # most_active_users_april_retweets_fig.show()

    # active_users_per_day_tweets_fig = most_active_users_per_day_tweets(db)
    # active_users_per_day_tweets_fig.show()
    #
    # tw_lang_per_day_fig = tw_lang_per_day(db)
    # tw_lang_per_day_fig.show()
    #
    # inf_tw_per_day_fig, inf_rtw_per_day_fig, inf_rep_per_day_fig, inf_q_per_day_fig = influencers_per_day(db)
    # inf_tw_per_day_fig.show()
    # inf_rtw_per_day_fig.show()
    # inf_rep_per_day_fig.show()
    # inf_q_per_day_fig.show()
    #
    # newly_created_accounts_fig = newly_created_accounts(db)
    # newly_created_accounts_fig.show()
    #
    # newly_created_accounts_pattern_fig = newly_created_accounts_pattern(db)
    # newly_created_accounts_pattern_fig.show()

    # sentiment_per_state_per_day_fig = sentiment_per_state_per_day(db)
    # sentiment_per_state_per_day_fig.show()

    # most_popular_hashtags_april_fig = most_popular_hashtags(db, *april)
    # most_popular_hashtags_april_fig.show()

    polish_sentiment_fig = polish_sentiment(db)
    polish_sentiment_fig.show()

    # polish_sentiment_with_daily_tweets_fig = polish_sentiment_with_daily_tweets(db)
    # polish_sentiment_with_daily_tweets_fig.show()