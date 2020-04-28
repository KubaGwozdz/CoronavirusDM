from datetime import datetime

import plotly.graph_objects as go

from db_utils.data_selector import DataSelector


def tweets_per_country(db):
    data = db.get_tweets_per_country()
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
        title_text='Number of tweets per country',
        geo=dict(
            showframe=False,
            showcoastlines=False,
            projection_type='equirectangular'
        )
    )
    return fig


def sentiment_per_country(db):
    data = db.get_sentiment_per_country(to_date=datetime.today())
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
        title_text='Social mood in tweets per country for March 2020',
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


def sentiment_per_state(db):
    data = db.get_sentiment_per_state(to_date=datetime.today())
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
        title_text='Social mood in tweets per state for march 2020',
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

    # tweets_per_country_fig = tweets_per_country(db)
    # tweets_per_country_fig.show()

    polarity_fig = sentiment_per_country(db)
    polarity_fig.show()

    polarity_USA_fig = sentiment_per_state(db)
    polarity_USA_fig.show()
