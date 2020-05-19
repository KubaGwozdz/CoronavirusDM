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
    def __init__(self, db, predict_range, training_period, timeline, extended_timeline, N, params, fit_data):
        self.db = db
        self.predict_range = predict_range
        self.training_period = training_period

        self.timeline = timeline
        self.extended_timeline = extended_timeline

        self.N = N
        self.params = params
        self.fit_data = fit_data

    @staticmethod
    def extend_timeline(dates: list, time_range: int):
        last_date = dates[-1]
        last_date = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S").date()
        for i in range(time_range):
            next_date = last_date + timedelta(days=1)
            dates.append(next_date.strftime("%Y-%m-%d %H:%M:%S"))
            last_date = next_date
        return dates

    def f_i(self, **kwargs):
        raise NotImplemented

    def update_params(self, params):
        raise NotImplemented

    def train(self):
        self.days = len(self.timeline[:self.training_period])
        x = np.linspace(0, self.days - 1, self.days, dtype=int)  # x_data is just [0, 1, ..., max_days] array

        model = lmfit.Model(self.f_i)
        # we set the parameters (and some initial parameter guesses)
        for param in self.params:
            name, value = param
            model.set_param_hint(name, value=value, vary=True)

        params = model.make_params()
        result = model.fit(self.fit_data[:self.training_period], params, method="leastsq", x=x)  # fitting
        params = result.best_values
        print(params)

        self.update_params(params)
        self.best_fit = result.best_fit

    def predict(self):
        raise NotImplemented

    def train_and_predict(self):
        self.train()
        return self.predict()


class SIS(EpidemicModel):  # from S to I and if recovered back to S
    def __init__(self, db, virus_name, country_name, predcit_range, training_period=-1):
        data = db.get_epidemic_data_in([country_name], ['deaths', 'confirmed', 'recovered'], virus_name,
                                       since_epidemy_start=True)

        timeline = data['dates']
        extended_timeline = timeline

        self.confirmed = data[country_name]['confirmed']
        self.recovered = data[country_name]['recovered']
        self.deaths = data[country_name]['deaths']

        self.active = [data[country_name]['confirmed'][i] - self.recovered[i] - self.deaths[i] for i in
                       range(len(self.deaths))]

        N = data[country_name]['population']

        self.I0 = 1
        self.S0 = N - self.I0
        self.Y0 = self.S0, self.I0

        self.beta = 0.2
        self.gamma = 0.1

        params = [
            ("beta", self.beta),
            ("gamma", self.gamma),
            ("i0", self.I0),
        ]

        super().__init__(db, predcit_range, training_period, timeline, extended_timeline, N, params, self.active)

    @staticmethod
    def deriv(y, t, N, beta, gamma):
        S, I = y
        dSdt = -(beta * S * I) / N + gamma * I
        dIdt = -(beta * S * I) / N - gamma * I
        return dSdt, dIdt

    def f_i(self, x, beta, gamma, i0):
        t = np.linspace(0, self.days, self.days)
        s0 = self.N - i0
        y0 = s0, i0
        ret = odeint(SIS.deriv, y0, t, args=(self.N, beta, gamma))
        S, I = ret.T
        return I[x]

    def update_params(self, params):
        self.beta = params['beta']
        self.gamma = params['gamma']
        self.I0 = params['i0']

        self.S0 = self.N - self.I0
        self.Y0 = self.S0, self.I0

    def predict(self):
        self.extended_timeline = self.extend_timeline(self.timeline, self.predict_range)
        size = len(self.extended_timeline)
        t = np.linspace(0, size, size)
        ret = odeint(self.deriv, self.Y0, t, args=(self.N, self.beta, self.gamma))
        S, I = ret.T
        return S, I


class SIR(EpidemicModel):
    def __init__(self, db, virus_name, country_name, predcit_range, training_period=-1):
        data = db.get_epidemic_data_in([country_name], ['deaths', 'confirmed', 'recovered'], virus_name,
                                       since_epidemy_start=True)

        timeline = data['dates']
        extended_timeline = timeline

        self.confirmed = data[country_name]['confirmed']
        self.recovered = data[country_name]['recovered']
        self.deaths = data[country_name]['deaths']

        self.active = [data[country_name]['confirmed'][i] - self.recovered[i] - self.deaths[i] for i in
                       range(len(self.deaths))]

        N = data[country_name]['population']

        self.I0 = 1
        self.R0 = 0
        self.S0 = N - self.I0 - self.R0
        self.Y0 = self.S0, self.I0, self.R0

        self.beta = 0.2
        self.gamma = 0.1

        params = [
            ("beta", self.beta),
            ("gamma", self.gamma),
            ("i0", self.I0),
            ("r0", self.R0),
        ]

        super().__init__(db, predcit_range, training_period, timeline, extended_timeline, N, params, self.active)

    @staticmethod
    def deriv(y, t, N, beta, gamma):
        S, I, R = y
        dSdt = -beta * S * I / N
        dIdt = beta * S * I / N - gamma * I
        dRdt = gamma * I
        return dSdt, dIdt, dRdt

    def f_i(self, x, beta, gamma, i0, r0):
        t = np.linspace(0, self.days, self.days)
        s0 = self.N - i0 - r0
        y0 = s0, i0, r0
        ret = odeint(self.deriv, y0, t, args=(self.N, beta, gamma))
        S, I, R = ret.T
        return I[x]

    def update_params(self, params):
        self.beta = params['beta']
        self.gamma = params['gamma']
        self.I0 = params['i0']
        self.R0 = params['r0']

        self.S0 = self.N - self.I0 - self.R0
        self.Y0 = self.S0, self.I0, self.R0

    def predict(self):
        self.extended_timeline = self.extend_timeline(self.timeline, self.predict_range)
        size = len(self.extended_timeline)
        t = np.linspace(0, size, size)
        ret = odeint(self.deriv, self.Y0, t, args=(self.N, self.beta, self.gamma))
        S, I, R = ret.T
        return S, I, R


class SEIR(EpidemicModel):  # from S to E and possibly to I then R and never back - immunity
    def __init__(self, db, virus_name, country_name, predcit_range, training_period=-1):
        data = db.get_epidemic_data_in([country_name], ['deaths', 'confirmed', 'recovered'], virus_name,
                                       since_epidemy_start=True)

        timeline = data['dates']
        extended_timeline = timeline

        self.confirmed = data[country_name]['confirmed']
        self.recovered = data[country_name]['recovered']
        self.deaths = data[country_name]['deaths']

        self.active = [data[country_name]['confirmed'][i] - self.recovered[i] - self.deaths[i] for i in
                       range(len(self.deaths))]

        N = data[country_name]['population']

        self.I0 = 655
        self.R0 = 34937575
        self.S0 = N - self.I0 - self.R0
        self.E0 = N - (self.S0 + self.I0 + self.R0)
        self.Y0 = self.S0, self.E0, self.I0, self.R0

        self.beta = 2.2
        self.gamma = 0.85
        self.mi = 0.2

        params = [
            ("beta", self.beta),
            ("gamma", self.gamma),
            ("mi", self.mi),
            ("i0", self.I0),
            ("r0", self.R0),
        ]

        super().__init__(db, predcit_range, training_period, timeline, extended_timeline, N, params, self.active)

    @staticmethod
    def deriv(y, t, N, beta, gamma, mi):
        S, E, I, R = y

        # dSdt = mi * N - mi * S - beta * (I / N) * S
        # dEdt = beta * (I / N) * S - (mi + alpha) * E
        # dIdt = alpha * E - (gamma + mi) * I
        # dRdt = gamma * I - mi * R

        dSdt = -beta * S * I / N
        dEdt = beta * S * I / N - mi * E
        dIdt = mi * E - gamma * I
        dRdt = gamma * I

        return dSdt, dEdt, dIdt, dRdt

    def f_i(self, x, beta, gamma, mi, i0, r0):
        t = np.linspace(0, self.days, self.days)
        s0 = self.N - i0 - r0
        e0 = self.N - (s0 + i0 + r0)

        y0 = s0, e0, i0, r0
        ret = odeint(self.deriv, y0, t, args=(self.N, beta, gamma, mi))
        S, E, I, R = ret.T
        return I[x]

    def update_params(self, params):
        self.beta = params['beta']
        self.gamma = params['gamma']
        self.mi = params['mi']
        self.I0 = params['i0']
        self.R0 = params['r0']

        self.S0 = self.N - self.I0 - self.R0
        self.E0 = self.N - (self.S0 + self.I0 + self.R0)
        self.Y0 = self.S0, self.E0, self.I0, self.R0

    def predict(self):
        self.extended_timeline = self.extend_timeline(self.timeline, self.predict_range)
        size = len(self.extended_timeline)
        t = np.linspace(0, size, size)
        ret = odeint(self.deriv, self.Y0, t, args=(self.N, self.beta, self.gamma, self.mi))
        S, E, I, R = ret.T
        return S, E, I, R


class SEIRD(EpidemicModel):  # from S to E and possibly to I then R and never back - immunity
    def __init__(self, db, virus_name, country_name, predcit_range, training_period=-1):
        data = db.get_epidemic_data_in([country_name], ['deaths', 'confirmed', 'recovered'], virus_name,
                                       since_epidemy_start=True)

        timeline = data['dates']
        extended_timeline = timeline

        self.confirmed = data[country_name]['confirmed']
        self.recovered = data[country_name]['recovered']
        self.deaths = data[country_name]['deaths']

        self.active = [data[country_name]['confirmed'][i] - self.recovered[i] - self.deaths[i] for i in
                       range(len(self.deaths))]

        N = data[country_name]['population']

        self.I0 = 655
        self.R0 = 34937575
        self.S0 = N - self.I0 - self.R0
        self.E0 = N - (self.S0 + self.I0 + self.R0)
        self.Y0 = self.S0, self.E0, self.I0, self.R0

        self.beta = 2.2
        self.gamma = 0.85
        self.mi = 0.2
        self.alpha = 7

        params = [
            ("beta", self.beta),
            ("gamma", self.gamma),
            ("mi", self.mi),
            ("alpha", self.alpha),
            ("i0", self.I0),
            ("r0", self.R0),
        ]

        super().__init__(db, predcit_range, training_period, timeline, extended_timeline, N, params, self.active)

    @staticmethod
    def deriv(y, t, N, beta, gamma, mi, alpha):
        S, E, I, R = y

        # dSdt = mi * N - mi * S - beta * (I / N) * S
        # dEdt = beta * (I / N) * S - (mi + alpha) * E
        # dIdt = alpha * E - (gamma + mi) * I
        # dRdt = gamma * I - mi * R

        dSdt = -beta * S * I / N
        dEdt = beta * S * I / N - mi * E
        dIdt = mi * E - gamma * I
        dRdt = gamma * I

        return dSdt, dEdt, dIdt, dRdt

    def f_i(self, x, beta, gamma, mi, alpha, i0, r0):
        t = np.linspace(0, self.days, self.days)
        s0 = self.N - i0 - r0
        e0 = self.N - (s0 + i0 + r0)

        y0 = s0, e0, i0, r0
        ret = odeint(self.deriv, y0, t, args=(self.N, beta, gamma, mi, alpha))
        S, E, I, R = ret.T
        return I[x]

    def update_params(self, params):
        self.beta = params['beta']
        self.gamma = params['gamma']
        self.mi = params['mi']
        self.alpha = params['alpha']
        self.I0 = params['i0']
        self.R0 = params['r0']

        self.S0 = self.N - self.I0 - self.R0
        self.E0 = self.N - (self.S0 + self.I0 + self.R0)
        self.Y0 = self.S0, self.E0, self.I0, self.R0

    def predict(self):
        self.extended_timeline = self.extend_timeline(self.timeline, self.predict_range)
        size = len(self.extended_timeline)
        t = np.linspace(0, size, size)
        ret = odeint(self.deriv, self.Y0, t, args=(self.N, self.beta, self.gamma, self.mi, self.alpha))
        S, E, I, R = ret.T
        return S, E, I, R

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
