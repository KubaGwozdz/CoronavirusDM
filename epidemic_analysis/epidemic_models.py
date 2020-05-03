import numpy as np
from scipy.integrate import odeint, solve_ivp
from scipy.optimize import minimize
from functools import partial
from datetime import datetime, timedelta

import lmfit
from lmfit.lineshapes import gaussian, lorentzian

'''

S - susceptibles, N - (I+R)
I - infectious
R - recovered with immunity
E - exposed, infected but not infectious, N - (S+I+R)
D - deceased
M - newborns, immune for the first few months

gamma - recover rate
beta - infection rate
mi - mortality rate
lambda - birth rate
alpha - incubation period
delta -
assumption - birth rate = death rate, mi = lambda, N is constant

'''


class EpidemicModel:
    def __init__(self, db, predict_range):
        self.derivs = []
        self.db = db
        self.predict_range = predict_range

    @staticmethod
    def extend_timeline(dates: list, time_range: int):
        last_date = dates[-1]
        last_date = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S").date()
        for i in range(time_range):
            next_date = last_date + timedelta(days=1)
            dates.append(next_date.strftime("%Y-%m-%d %H:%M:%S"))
            last_date = next_date
        return dates


class SIS:  # from S to I and if recovered back to S
    def __init__(self, S, I, N, gamma, beta):
        dSdt = -(beta * S * I) / N + gamma * I
        dIdt = -(beta * S * I) / N - gamma * I
        self.derivs = [dSdt, dIdt]


class SIR(EpidemicModel):
    def __init__(self, db, virus_name, country_name, predcit_range):
        super().__init__(db, predcit_range)
        to_date = datetime.strptime("2020-03-15", "%Y-%m-%d").date()
        data = db.get_epidemic_data_in([country_name], ['deaths', 'confirmed', 'recovered'], virus_name,
                                       since_epidemy_start=True)

        self.timeline = data['dates']
        self.extended_timeline = self.timeline

        self.confirmed = data[country_name]['confirmed']
        self.recovered = data[country_name]['recovered']
        self.deaths = data[country_name]['deaths']

        self.active = [data[country_name]['confirmed'][i] - self.recovered[i] - self.deaths[i] for i in
                       range(len(self.deaths))]

        self.N = data[country_name]['population']

        self.I0 = 1
        self.R0 = 0
        self.S0 = self.N - self.I0 - self.R0
        self.Y0 = self.S0, self.I0, self.R0

        self.beta = 0.2
        self.gamma = 0.1

    @staticmethod
    def deriv(y, t, N, beta, gamma):
        S, I, R = y
        dSdt = -beta * S * I / N
        dIdt = beta * S * I / N - gamma * I
        dRdt = gamma * I
        return dSdt, dIdt, dRdt

    def train(self):
        # self.extended_timeline = self.extend_timeline(self.timeline, self.predict_range)
        days = len(self.timeline)
        x = np.linspace(0, days - 1, days, dtype=int)  # x_data is just [0, 1, ..., max_days] array

        def f_i(x, beta, gamma, i0, r0):
            t = np.linspace(0, days, days)
            s0 = self.N - i0 - r0
            y0 = s0, i0, r0
            ret = odeint(SIR20.deriv, y0, t, args=(self.N, beta, gamma))
            S, I, R = ret.T
            return I[x]

        model = lmfit.Model(f_i)
        # we set the parameters (and some initial parameter guesses)
        model.set_param_hint("beta", value=self.beta, vary=True)
        model.set_param_hint("gamma", value=self.gamma, vary=True)
        model.set_param_hint("i0", value=self.I0, vary=True)
        model.set_param_hint("r0", value=self.R0, vary=True)

        params = model.make_params()
        result = model.fit(self.active, params, method="leastsq", x=x)  # fitting
        params = result.best_values
        self.beta = params['beta']
        self.gamma = params['gamma']
        self.i0 = params['i0']
        self.r0 = params['r0']

        self.best_fit = result.best_fit

    def integrate(self):
        self.extended_timeline = self.extend_timeline(self.timeline, self.predict_range)
        size = len(self.extended_timeline)
        t = np.linspace(0, size, size)
        ret = odeint(SIR20.deriv, self.Y0, t, args=(self.N, self.beta, self.gamma))
        S, I, R = ret.T
        return S, I, R

    def train_and_predict(self):
        self.train()
        return self.integrate()



class SEIR:  # from S to E and possibly to I then R and never back - immunity

    def __init__(self, S, E, I, R, N, gamma, beta, mi, alpha):
        self.E = N - (S + I + R)

        self.dSdt = mi * N - mi * S - beta * (I / N) * S
        self.dEdt = beta * (I / N) * S - (mi + alpha) * E
        self.dIdt = alpha * E - (gamma + mi) * I
        self.dRdt = gamma * I - mi * R


class SEIS:  # like SEIR but without immunity

    def __init__(self, S, E, I, N, gamma, beta, mi, alpha):
        self.dSdt = mi * N - mi * S - beta * (I / N) * S
        self.dEdt = beta * (I / N) * S - (mi + alpha) * E
        self.dIdt = alpha * E - (gamma + mi) * I


class SIRD:  # like SIR + mi but differentiates between deceased and recovered

    def __init__(self, S, I, N, gamma, beta, mi):
        self.dSdt = -(beta * S * I) / N
        self.dIdt = (beta * S * I) / N - gamma * I - mi * I
        self.dRdt = gamma * I
        self.dDdt = mi * I
