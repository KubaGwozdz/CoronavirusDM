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
    data = db.get_sentiment_per_country()
    polarity_fig = go.Figure(data=go.Choropleth(
        locations=data['country_code'],
        locationmode='ISO-3',
        z=data['polarity'],
        text=data['country_name'],
        # colorscale='Blues',
        autocolorscale=True,
        # reversescale=True,
        marker_line_color='darkgray',
        marker_line_width=0.5,
        colorbar_title='Tweet polarity: 1 - positive, -1 - negative',
    ))

    polarity_fig.update_layout(
        title_text='Social mood in tweets per country',
        geo=dict(
            showframe=False,
            showcoastlines=False,
            projection_type='equirectangular'
        )
    )

    subjectivity_fig = go.Figure(data=go.Choropleth(
        locations=data['country_code'],
        locationmode='ISO-3',
        z=data['subjectivity'],
        text=data['country_name'],
        # colorscale='Blues',
        autocolorscale=True,
        # reversescale=True,
        marker_line_color='darkgray',
        marker_line_width=0.5,
        colorbar_title='Tweet polarity: 1 - subjective, 0 - objective',
    ))

    subjectivity_fig.update_layout(
        title_text='Social mood in tweets per country',
        geo=dict(
            showframe=False,
            showcoastlines=False,
            projection_type='equirectangular'
        )
    )
    return polarity_fig, subjectivity_fig

if __name__ == "__main__":
    db = DataSelector()

    # tweets_per_country_fig = tweets_per_country(db)
    # tweets_per_country_fig.show()

    polarity_fig, subjectivity_fig = sentiment_per_country(db)
    polarity_fig.show()
    subjectivity_fig.show()