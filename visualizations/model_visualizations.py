import plotly.graph_objects as go
from plotly.subplots import make_subplots


from db_utils.data_selector import DataSelector
from epidemic_analysis.epidemic_models import SIS, SIR, SEIR, SEIRD, TwitterModel

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


def show_tweets_with_pandemic(tw_model, country_name):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(
            x=tw_model.timeline,
            y=tw_model.tweets,
            mode="lines",
            name="number of tweets"
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

    fig.update_xaxes(title_text="date")

    fig.update_yaxes(title_text="Number of people", secondary_y=False)
    fig.update_yaxes(title_text="Number of tweets", secondary_y=True)

    fig.update_layout(
        title_text="COVID19 in " + country_name
    )
    fig.show()


if __name__ == "__main__":
    db = DataSelector()

    # ------ COVID19 ------

    # c19_infected_in_POL_fig = affected_in(["Poland", "Italy"], ["deaths", 'confirmed'], "COVID19", db)
    # c19_infected_in_POL_fig.show()

    # country = COUNTRY_NAME, STATE_NAME=None
    # country = "US", ''
    # country = "Poland", None
    # country = "Italy", None
    country = "India", None

    prediction_period = 100

    # SIR_predictction_fig, SIR_model = SIR_predicted_in(country, "COVID19", prediction_period, db, SIR)
    # SIR_predictction_fig.show()
    #
    # init_vals = SIR_model.beta, SIR_model.gamma, SIR_model.I0
    # SEIR_predictction_fig, SEIR_model = SEIR_predicted_in(country, "COVID19", prediction_period, db, SEIR, init_vals)
    # SEIR_predictction_fig.show()
    #
    # init_vals = SIR_model.beta, SIR_model.gamma, SEIR_model.delta, SIR_model.I0
    # SEIRD_predictction_fig = SEIRD_predicted_in(country, "COVID19", prediction_period, db, SEIRD, init_vals)
    # SEIRD_predictction_fig.show()



    # tw_country = "Italy", None
    # tw_country = "Poland", None
    tw_country = "US", None
    # tw_country = "US", 'NY'
    country_name, state = tw_country
    if state is not None:
        country_name += ', ' + state
    tw_model = TwitterModel(db, prediction_period, *tw_country)
    show_tweets_with_pandemic(tw_model, country_name)
