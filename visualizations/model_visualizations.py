import plotly.graph_objects as go

from db_utils.data_selector import DataSelector


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



if __name__ == "__main__":
    db = DataSelector()

    # ------ COVID19 ------

    c19_infected_in_POL_fig = affected_in(["Poland", "Italy"], ["deaths", 'confirmed'], "COVID19", db)
    c19_infected_in_POL_fig.show()