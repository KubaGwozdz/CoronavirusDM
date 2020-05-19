from datetime import datetime

import plotly.graph_objects as go

from db_utils.data_selector import DataSelector


def tweets_per_country(db, from_date=None, to_date=None, month_name=None):
    data = db.get_tweets_per_country(from_date, to_date)
    fig = go.Figure(data=go.Choropleth(
        locations=data['country_code'],
        locationmode='ISO-3',
        z=data['number'],
        text=data['country_name'],
        colorscale='Blues',
        autocolorscale=False,
        reversescale=True,
        marker_line_color='darkgray',
        marker_line_width=0.5,
        colorbar_title='Tweets in country',
    ))

    fig.update_layout(
        title_text='Number of tweets per country: ' + month_name,
        geo=dict(
            showframe=False,
            showcoastlines=False,
            projection_type='equirectangular'
        )
    )
    return fig


def sentiment_per_country(db, from_date=None, to_date=None, month_name=None):
    data = db.get_sentiment_per_country(from_date, to_date)
    polarity_fig = go.Figure(data=go.Choropleth(
        locations=data['country_code'],
        locationmode='ISO-3',
        z=data['polarity'],
        text=data['country_name'],
        colorscale='RdYlGn',
        autocolorscale=False,
        marker_line_color='darkgray',
        marker_line_width=0.5,
        colorbar_title='Tweet polarity: 1 - positive, -1 - negative',
    ))

    polarity_fig.update_layout(
        title_text='Social mood in tweets per country: ' + month_name,
        geo=dict(
            showframe=False,
            showcoastlines=False,
            projection_type='equirectangular'
        )
    )
    polarity_fig.update_layout(
        geo=go.layout.Geo(
            # range_color=(-1, 1)
        )
    )

    return polarity_fig


def sentiment_per_state(db, from_date=None, to_date=None, month_name=None):
    data = db.get_sentiment_per_state(from_date, to_date)
    print(data)
    polarity_fig = go.Figure(data=go.Choropleth(
        locations=data['state_code'],
        locationmode='USA-states',
        z=data['polarity'],
        text=data['state_name'],
        colorscale='RdYlGn',
        autocolorscale=False,
        marker_line_color='darkgray',
        marker_line_width=0.5,
        colorbar_title='Tweet polarity: 1 - positive, -1 - negative',
    ))

    polarity_fig.update_layout(
        title_text='Social mood in tweets per state: ' + month_name,
        geo=dict(
            showframe=False,
            showcoastlines=False,
            projection_type='equirectangular'
        )
    )
    polarity_fig.update_layout(
        geo=go.layout.Geo(
            scope='usa',
            # range_color=(-1, 1)
        )
    )

    return polarity_fig


if __name__ == "__main__":
    db = DataSelector()
    march = datetime.strptime("2020-03-01", "%Y-%m-%d").date(), datetime.strptime("2020-03-31",
                                                                                  "%Y-%m-%d").date(), "march"
    april = datetime.strptime("2020-04-01", "%Y-%m-%d").date(), datetime.strptime("2020-04-30",
                                                                                  "%Y-%m-%d").date(), "april"

    # tweets_per_country_fig_march = tweets_per_country(db, *march)
    # tweets_per_country_fig_march.show()
    #
    # tweets_per_country_fig_april = tweets_per_country(db, *april)
    # tweets_per_country_fig_april.show()
    #
    # polarity_fig_march = sentiment_per_country(db, *march)
    # polarity_fig_march.show()
    #
    # polarity_fig_april = sentiment_per_country(db, *april)
    # polarity_fig_april.show()

    polarity_USA_fig_march = sentiment_per_state(db, *march)
    polarity_USA_fig_march.show()

    polarity_USA_fig_april = sentiment_per_state(db, *april)
    polarity_USA_fig_april.show()
