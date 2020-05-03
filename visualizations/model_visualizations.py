import plotly.graph_objects as go

from db_utils.data_selector import DataSelector
from epidemic_analysis.epidemic_models import SIR

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
        title_text = " and ".join(columns)
    )
    return fig

def predicted_in(country_name, virus_name, predict_range, db):
    model = SIR(db, virus_name, country_name, predict_range)
    S,I,R = model.train_and_predict()
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
                             name='R'
                             ))
    fig.add_trace(go.Scatter(x=model.timeline,
                             y=model.active,
                             mode='lines',
                             name="active"))
    return fig






if __name__ == "__main__":
    db = DataSelector()

    # ------ COVID19 ------

    # c19_infected_in_POL_fig = affected_in(["Poland", "Italy"], ["deaths", 'confirmed'], "COVID19", db)
    # c19_infected_in_POL_fig.show()

    predictction_fig = predicted_in("Italy", "COVID19", 100, db)
    predictction_fig.show()
