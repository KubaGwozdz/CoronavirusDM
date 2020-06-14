import numpy as np
from scipy.integrate import odeint, solve_ivp
from scipy.optimize import minimize
from functools import partial
from datetime import datetime, timedelta

import lmfit
from lmfit.lineshapes import gaussian, lorentzian

from epidemic_analysis.epidemic_models import EpidemicModel

'''

S - susceptibles, no opinion, don't know about topic
I - infected, have opinion:
    Pp - positive
    N - neutral
    Pm - negative
    
    I = Pp + N + Pm
R - recovered, lost interest in topic

'''


class sentiment_SIR(EpidemicModel):
    def __init__(self, db, country_name, predict_range, training_period=-1):

        data = db.get_sentiment_data_in(country_name)

        timeline = data['dates']
        extended_timeline = timeline

        self.positive = data[country_name]['positive']
        self.neutral = data[country_name]['neutral']
        self.negative = data[country_name]['negative']

        self.active = [self.neutral[i] + self.positive[i] + self.negative[i] for i in
                       range(len(self.positive))]

        N = data[country_name]['users']

        self.Pp0 = 0
        self.N0 = 1
        self.Pm0 = 0

        self.I0 = self.Pp0 + self.N0 + self.Pm0
        self.R0 = 0
        self.S0 = int(N[0][0]) - self.I0 - self.R0
        self.Y0 = self.S0, self.Pp0, self.N0, self.Pm0, self.R0

        self.beta_PpN = 0.4
        self.beta_PpPm = 0.1
        self.beta_NPp = 0.3
        self.beta_NPm = 0.35
        self.beta_PmN = 0.5
        self.beta_PmPp = 0.05

        self.alfa = 0.2
        self.gamma = 0.1


        params = [
            ("alfa", self.alfa),
            ("beta_PpN", self.beta_PpN),
            ("beta_PpPm", self.beta_PpPm),
            ("beta_NPp", self.beta_NPp),
            ("beta_NPm", self.beta_NPm),
            ("beta_PmN", self.beta_PmN),
            ("beta_PmPp", self.beta_PmPp),
            ("gamma", self.gamma),
            ("Pp0", self.Pp0),
            ("N0", self.N0),
            ("Pm0", self.Pm0),
            ("r0", self.R0),
        ]

        super().__init__(db, predict_range, training_period, timeline, extended_timeline, N, params, self.active)

    @staticmethod
    def deriv(y, t, N, alfa, beta_PpN, beta_PpPm, beta_NPp, beta_NPm, beta_PmN, beta_PmPp, gamma):
        S, Pp, N, Pm, R = y
        I = Pp + N + Pm
        dPpdt = -beta_PpN * Pp * N / I - beta_PpPm * Pp * Pm / I + beta_NPp * N * Pp / I + beta_PmPp * Pm * Pp / I
        dNdt = -beta_NPp * N * Pp / I - beta_NPm * N * Pm / I + beta_PpN * Pp * N / I + beta_PmN * Pm * N / I
        dPmdt = -beta_PmN * Pm * N / I - beta_PmPp * Pm * Pp / I + beta_PmN * Pm * N / I + beta_PpPm * Pp * Pm / I
        dSdt = -alfa * S * I / N
        dRdt = gamma * I
        return dSdt, dPpdt, dNdt, dPmdt, dRdt

    def f_i(self, x, alfa, beta_PpN, beta_PpPm, beta_NPp, beta_NPm, beta_PmN, beta_PmPp, gamma, pp0, n0, pm0, r0):
        t = np.linspace(0, self.days, self.days)
        i0 = pp0 + n0 + pm0
        s0 = self.N - i0 - r0
        y0 = s0, pp0, n0, pm0, r0
        ret = odeint(self.deriv, y0, t, args=(self.N, alfa, beta_PpN, beta_PpPm, beta_NPp, beta_NPm, beta_PmN, beta_PmPp, gamma))
        S, Pp, N, Pm, R = ret.T
        return N[x]

    def update_params(self, params):
        self.alfa = params['alfa']
        self.gamma = params['gamma']
        self.beta_PpN = params['beta_PpN']
        self.beta_PpPm = params['beta_PpPm']
        self.beta_NPp = params['beta_NPp']
        self.beta_NPm = params['beta_NPm']
        self.beta_PmN = params['beta_PmN']
        self.beta_PmPp = params['beta_PmPp']

        self.R0 = params['r0']
        self.Pp0 = params['Pp0']
        self.N0 = params['N0']
        self.Pm0 = params['Pm0']

        I0 = self.Pp0 + self.N0 + self.Pm0

        self.S0 = self.N - I0 - self.R0
        self.Y0 = self.S0, self.Pp0, self.N0, self.Pm0, self.R0

    def predict(self):
        self.extended_timeline = self.extend_timeline(self.timeline, self.predict_range)
        size = len(self.extended_timeline)
        t = np.linspace(0, size, size)
        ret = odeint(self.deriv, self.Y0, t, args=(self.N, self.beta_PpN, self.beta_PpPm, self.beta_NPp, self.beta_NPm,
                                                   self.beta_PmN, self.beta_PmPp, self.alfa, self.gamma))
        S, Pp, N, Pm, R = ret.T
        return S, Pp, N, Pm, R
