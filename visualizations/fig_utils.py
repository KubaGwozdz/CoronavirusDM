from plotly import offline
import plotly.graph_objects as go


def save_fig(fig: go.Figure, filename: str):
    offline.plot(fig, filename=filename+'.html', auto_open=False)
