import plotly.graph_objects as go
from plotly.subplots import make_subplots


from db_utils.data_selector import DataSelector
from sentiment.sentiment_model import sentiment_SIR
from epidemic_analysis.epidemic_models import SIS, SIR, SEIR, SEIRD, TwitterModel, TwitterSentimentModel

import lmfit
from lmfit.lineshapes import gaussian, lorentzian
import matplotlib.pyplot as plt
import numpy as np


def affected_in(countries_names: list, columns: list, virus_name, db):
    data = db.get_epidemic_data_in(countries_names, columns, virus_name)
    fig = go.Figure()
    for c_name in countries_names:
        for col in columns:
            fig.add_trace(go.Scatter(x=data['dates'],
                                     y=data[c_name][col],
                                     mode='lines',
                                     name=c_name + '_' + col))
    fig.update_layout(
        title_text=" and ".join(columns)
    )
    return fig


def SIR_predicted_in(country, virus_name, predict_range, db, pandemic_model):
    country_name, state_name = country
    model = pandemic_model(db, virus_name, country_name, predict_range, state_name=state_name)
    S, I, R = model.train_and_predict()
    fig = go.Figure()

    # fig.add_trace(go.Scatter(x=model.timeline,
    #                          y=model.best_fit,
    #                          mode='lines',
    #                          name='best_fit'
    #                          ))
    fig.add_trace(go.Scatter(x=model.extended_timeline,
                             y=S,
                             mode='lines',
                             name="S"
                             ))
    fig.add_trace(go.Scatter(x=model.extended_timeline,
                             y=I,
                             mode='lines',
                             name="I"
                             ))
    fig.add_trace(go.Scatter(x=model.extended_timeline,
                             y=R,
                             mode='lines',
                             name="R"
                             ))
    fig.add_trace(go.Scatter(x=model.timeline,
                             y=model.active,
                             mode='lines',
                             name="active"))
    fig.update_layout(
        title_text="SIR: " + country_name
    )
    return fig, model


def SEIR_predicted_in(country, virus_name, predict_range, db, pandemic_model, init_vals):
    beta, gamma, i0 = init_vals
    country_name, state_name = country
    model = pandemic_model(db, virus_name, country_name, predict_range, beta=beta, gamma=gamma, i0=i0,
                           state_name=state_name)
    S, E, I, R = model.train_and_predict()
    # S, E, I, R = model.fine_tune_and_predict()
    fig = go.Figure()

    # fig.add_trace(go.Scatter(x=model.timeline,
    #                          y=model.best_fit,
    #                          mode='lines',
    #                          name='best_fit'
    #                          ))
    fig.add_trace(go.Scatter(x=model.extended_timeline,
                             y=S,
                             mode='lines',
                             name="S"
                             ))
    fig.add_trace(go.Scatter(x=model.extended_timeline,
                             y=E,
                             mode='lines',
                             name="E"
                             ))
    fig.add_trace(go.Scatter(x=model.extended_timeline,
                             y=I,
                             mode='lines',
                             name="I"
                             ))
    fig.add_trace(go.Scatter(x=model.extended_timeline,
                             y=R,
                             mode='lines',
                             name="R"
                             ))
    fig.add_trace(go.Scatter(x=model.timeline,
                             y=model.active,
                             mode='lines',
                             name="active"))
    fig.update_layout(
        title_text="SEIR: " + country_name
    )
    return fig, model


def SEIRD_predicted_in(country, virus_name, predict_range, db, pandemic_model, init_vals):
    beta, gamma, delta, i0 = init_vals
    country_name, state_name = country
    model = pandemic_model(db, virus_name, country_name, predict_range, beta=beta, gamma=gamma, delta=delta, i0=i0,
                           state_name=state_name)

    S, E, I, R, D = model.train_and_predict()
    # S, E, I, R, D = model.fine_tune_and_predict()
    fig = go.Figure()

    # fig.add_trace(go.Scatter(x=model.timeline,
    #                          y=model.best_fit,
    #                          mode='lines',
    #                          name='best_fit'
    #                          ))
    fig.add_trace(go.Scatter(x=model.extended_timeline,
                             y=S,
                             mode='lines',
                             name="S"
                             ))
    fig.add_trace(go.Scatter(x=model.extended_timeline,
                             y=E,
                             mode='lines',
                             name="E"
                             ))
    fig.add_trace(go.Scatter(x=model.extended_timeline,
                             y=I,
                             mode='lines',
                             name="I"
                             ))
    fig.add_trace(go.Scatter(x=model.extended_timeline,
                             y=R,
                             mode='lines',
                             name="R"
                             ))
    fig.add_trace(go.Scatter(x=model.extended_timeline,
                             y=D,
                             mode='lines',
                             name="D"
                             ))
    fig.add_trace(go.Scatter(x=model.timeline,
                             y=model.active,
                             mode='lines',
                             name="active"))
    fig.add_trace(go.Scatter(x=model.timeline,
                             y=model.deaths,
                             mode='lines',
                             name="deaths_data"))
    fig.update_layout(
        title_text="SEIRD: " + country_name
    )
    return fig


def show_SEIR(model, country_name, S, E, I, R):
    fig = go.Figure()

    fig.add_trace(go.Scatter(x=model.timeline,
                             y=model.best_fit,
                             mode='lines',
                             name='best_fit'
                             ))
    fig.add_trace(go.Scatter(x=model.extended_timeline,
                             y=S,
                             mode='lines',
                             name="S"
                             ))
    fig.add_trace(go.Scatter(x=model.extended_timeline,
                             y=E,
                             mode='lines',
                             name="E"
                             ))
    fig.add_trace(go.Scatter(x=model.extended_timeline,
                             y=I,
                             mode='lines',
                             name="I"
                             ))
    fig.add_trace(go.Scatter(x=model.extended_timeline,
                             y=R,
                             mode='lines',
                             name="R"
                             ))
    fig.add_trace(go.Scatter(x=model.timeline,
                             y=model.active,
                             mode='lines',
                             name="active"))
    fig.update_layout(
        title_text="SEIR: " + country_name
    )
    fig.show()



def show_tweets_with_pandemic(tw_model, country_name):
    S, E, I, R, T = tw_model.train_and_predict()
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(
            x=tw_model.timeline,
            y=tw_model.tweets,
            name="number of tweets",
            marker_color="rgb(51,153,255)",
            opacity=0.5
        ),
        secondary_y=True
    )
    fig.add_trace(
        go.Bar(
            x=tw_model.timeline,
            y=tw_model.positive_tweets,
            name="number of positive tweets",
            marker_color="rgb(102,189,99)",
            opacity=0.5
        ),
        secondary_y=True
    )
    fig.add_trace(
        go.Bar(
            x=tw_model.timeline,
            y=tw_model.negative_tweets,
            name="number of negative tweets",
            marker_color="rgb(215,48,39)",
            opacity=0.5
        ),
        secondary_y=True
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.timeline,
            y=tw_model.active,
            mode='lines',
            name="active"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.timeline,
            y=tw_model.confirmed,
            mode='lines',
            name="confirmed"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.timeline,
            y=tw_model.recovered,
            mode='lines',
            name="recovered"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.timeline,
            y=tw_model.deaths,
            mode='lines',
            name="deaths"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.extended_timeline,
            y=S,
            mode='lines',
            name="S"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.extended_timeline,
            y=E,
            mode='lines',
            name="E"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.extended_timeline,
            y=I,
            mode='lines',
            name="I"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.extended_timeline,
            y=R,
            mode='lines',
            name="R"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.extended_timeline,
            y=T,
            mode='lines',
            name="T"),
        secondary_y=True
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.timeline,
            y=tw_model.tweets_line,
            mode='lines',
            name="tweets affecting"),
        secondary_y=True
    )

    fig.update_xaxes(title_text="date")

    fig.update_yaxes(title_text="Number of people", secondary_y=False)
    fig.update_yaxes(title_text="Number of tweets", secondary_y=True)

    fig.update_layout(
        title_text="SEIR_T: " + country_name,
    )
    fig.show()


def show_tweets_sentiment_with_pandemic(tw_model, country_name):
    S, E, I, R, TP, TN = tw_model.train_and_predict()
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(
            x=tw_model.timeline,
            y=tw_model.tweets,
            name="number of tweets",
            marker_color="rgb(51,153,255)",
            opacity=0.5
        ),
        secondary_y=True
    )
    fig.add_trace(
        go.Bar(
            x=tw_model.timeline,
            y=tw_model.positive_tweets,
            name="number of positive tweets",
            marker_color="rgb(102,189,99)",
            opacity=0.5
        ),
        secondary_y=True
    )
    fig.add_trace(
        go.Bar(
            x=tw_model.timeline,
            y=tw_model.negative_tweets,
            name="number of negative tweets",
            marker_color="rgb(215,48,39)",
            opacity=0.5
        ),
        secondary_y=True
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.timeline,
            y=tw_model.active,
            mode='lines',
            name="active"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.timeline,
            y=tw_model.confirmed,
            mode='lines',
            name="confirmed"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.timeline,
            y=tw_model.recovered,
            mode='lines',
            name="recovered"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.timeline,
            y=tw_model.deaths,
            mode='lines',
            name="deaths"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.extended_timeline,
            y=S,
            mode='lines',
            name="S"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.extended_timeline,
            y=E,
            mode='lines',
            name="E"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.extended_timeline,
            y=I,
            mode='lines',
            name="I"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.extended_timeline,
            y=R,
            mode='lines',
            name="R"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.extended_timeline,
            y=TP,
            mode='lines',
            name="TP"),
        secondary_y=True
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.extended_timeline,
            y=TN,
            mode='lines',
            name="TN"),
        secondary_y=True
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.timeline,
            y=tw_model.tweets_line,
            mode='lines',
            name="tweets affecting"),
        secondary_y=True
    )

    fig.update_xaxes(title_text="date")

    fig.update_yaxes(title_text="Number of people", secondary_y=False)
    fig.update_yaxes(title_text="Number of tweets", secondary_y=True)

    fig.update_layout(
        title_text="SEIR_TP_TN: " + country_name,
    )
    fig.show()


def show_tweets_sentiment_model(tw_model, country_name):
    S, Pp, N, Pm, R = tw_model.predict()
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(
            x=tw_model.timeline,
            y=tw_model.positive,
            name="number of positive tweets",
            marker_color="rgb(102,189,99)",
            opacity=0.5
        ),
        secondary_y=True
    )
    fig.add_trace(
        go.Bar(
            x=tw_model.timeline,
            y=tw_model.negative,
            name="number of negative tweets",
            marker_color="rgb(215,48,39)",
            opacity=0.5
        ),
        secondary_y=True
    )
    fig.add_trace(
        go.Bar(
            x=tw_model.timeline,
            y=tw_model.neutral,
            name="number of neutral tweets",
            marker_color="rgb(51,153,255)",
            opacity=0.5
        ),
        secondary_y=True
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.extended_timeline,
            y=S,
            mode='lines',
            name="S"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.extended_timeline,
            y=R,
            mode='lines',
            name="R"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.extended_timeline,
            y=Pp,
            mode='lines',
            name="Pp"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.extended_timeline,
            y=Pm,
            mode='lines',
            name="Pm"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=tw_model.extended_timeline,
            y=N,
            mode='lines',
            name="N"),
        secondary_y=False
    )

    fig.update_xaxes(title_text="date")

    fig.update_yaxes(title_text="Number of tweets", secondary_y=True)

    fig.update_layout(
        title_text="S_PpNPm_R: " + country_name,
    )
    fig.show()


if __name__ == "__main__":
    db = DataSelector()

    # ------ COVID19 ------

    # c19_infected_in_POL_fig = affected_in(["Poland", "Italy"], ["deaths", 'confirmed'], "COVID19", db)
    # c19_infected_in_POL_fig.show()

    # SIR_predictction_fig = SIR_predicted_in("Italy", "COVID19", 100, db, SIR)
    # SIR_predictction_fig.show()

    # SEIR_predictction_fig = SEIR_predicted_in("Italy", "COVID19", 100, db, SEIR)
    # SEIR_predictction_fig.show()

    # country = COUNTRY_NAME, STATE_NAME=None
    country = "Poland", None
    # country = "Italy", None
    # country = "India", None

    prediction_period = 100

    # SIR_predictction_fig, SIR_model = SIR_predicted_in(country, "COVID19", prediction_period, db, SIR)
    # SIR_predictction_fig.show()

    # init_vals = SIR_model.beta, SIR_model.gamma, SIR_model.I0
    # SEIR_predictction_fig, SEIR_model = SEIR_predicted_in(country, "COVID19", prediction_period, db, SEIR, init_vals)
    # SEIR_predictction_fig.show()

    # init_vals = SIR_model.beta, SIR_model.gamma, SEIR_model.delta, SIR_model.I0
    # SEIRD_predictction_fig = SEIRD_predicted_in(country, "COVID19", prediction_period, db, SEIRD, init_vals)
    # SEIRD_predictction_fig.show()



    # tw_country = "Italy", None
    tw_country = "Poland", None
    # tw_country = "US", None
    # tw_country = "US", 'NY'
    country_name, state = tw_country
    country_displayname = country_name
    if state is not None:
        country_displayname += ', ' + state

    SEIR_model = SEIR(db, "COVID19", country_name, prediction_period, state_name=state)
    SEIR_model.train()
    SEIR = SEIR_model.predict()
    show_SEIR(SEIR_model, country_displayname, *SEIR)

    tw_model = TwitterSentimentModel(db, prediction_period, *tw_country)
    tw_model.initialize(SEIR_model.beta, SEIR_model.gamma, SEIR_model.delta, SEIR_model.I0)
    show_tweets_sentiment_with_pandemic(tw_model, country_displayname)

    SENTIMENT_test = db.get_sentiment_data_in(country_code='us')
    print(SENTIMENT_test)
