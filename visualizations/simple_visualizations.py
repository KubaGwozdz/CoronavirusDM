from datetime import datetime

from db_utils.data_selector import DataSelector
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


def retweets_per_day(db):
    data = db.get_number_of_retweets_per_day()
    X = [datetime.strptime(d, "%Y-%m-%d %H:%M:%S").date() for d in data.keys()]
    Y = list(data.values())

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=X,
        y=Y,
        line_color='deepskyblue',
        opacity=0.8))
    fig.update_layout(
        title_text="Retweets per day",
        xaxis_title="date",
        yaxis_title="number of retweets"
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


def most_popular_hashtags(db):
    data = db.get_most_popular_hashtags()
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
        title_text="Most popular hashtags",
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


def most_active_users_retweets(db):
    data = db.get_most_active_users_retweets()

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
    colors = ['#E74C3C', '#9B59B6' , '#2980B9', '#3498DB', '#1ABC9C',
              '#16A085', '#27AE60', '#F1C40F', '#F39C12', '#E12FBD',
              '#05E5F8', '#33F805', '#154EE8', '#941717', '#F49EE7']
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


if __name__ == "__main__":
    db = DataSelector()
    # tweets_per_day_fig = tweets_per_day(db)
    # tweets_per_day_fig.show()

    # retweets_per_day_fig = retweets_per_day(db)
    # retweets_per_day_fig.show()

    # tags_per_day_fig = most_popular_hashtags_per_day(db)
    # tags_per_day_fig.show()

    # popular_users_followers, popular_users_friends = popular_users(db)
    # popular_users_followers.show()
    # popular_users_friends.show()

    # active_users_fig = most_active_users(db)
    # active_users_fig.show()

    # most_active_users_retweets_fig = most_active_users_retweets(db)
    # most_active_users_retweets_fig.show()

    # most_popular_hashtags_fig = most_popular_hashtags(db)
    # most_popular_hashtags_fig.show()

    # active_users_per_day_tweets_fig = most_active_users_per_day_tweets(db)
    # active_users_per_day_tweets_fig.show()

    # tw_lang_per_day_fig = tw_lang_per_day(db)
    # tw_lang_per_day_fig.show()

    inf_tw_per_day_fig, inf_rtw_per_day_fig, inf_rep_per_day_fig, inf_q_per_day_fig = influencers_per_day(db)
    inf_tw_per_day_fig.show()
    inf_rtw_per_day_fig.show()
    inf_rep_per_day_fig.show()
    inf_q_per_day_fig.show()

    # newly_created_accounts_fig = newly_created_accounts(db)
    # newly_created_accounts_fig.show()

    # newly_created_accounts_pattern_fig = newly_created_accounts_pattern(db)
    # newly_created_accounts_pattern_fig.show()
