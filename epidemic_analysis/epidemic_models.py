import numpy as np
from scipy.integrate import odeint, solve_ivp
from scipy.optimize import minimize
from functools import partial
from datetime import datetime, timedelta

import lmfit
from lmfit.lineshapes import gaussian, lorentzian

import country_converter as coco

from math import exp

'''

S - susceptibles, N - (I+R)
I - infectious
R - recovered with immunity
E - exposed, infected but not infectious, N - (S+I+R)
D - deaths
M - newborns, immune for the first few months

--------------------------------------------------------------------------
D - number of days that an infected person has and can spread the disease
R0 = beta * D


gamma - recover rate
1/D


beta - infection rate
the expected amount of people an infected person infects per day



delta - incubation period 
alpha - death rate
rho - days from infection until death


mi - mortality rate
lambda - birth rate
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

    def fit_fun(self, **kwargs):
        raise NotImplemented

    def update_params(self, params):
        raise NotImplemented

    def train(self):
        self.days = len(self.timeline[:self.training_period])
        x = np.linspace(0, self.days - 1, self.days, dtype=int)  # x_data is just [0, 1, ..., max_days] array

        model = lmfit.Model(self.fit_fun)
        # we set the parameters (and some initial parameter guesses)
        for param in self.params:
            name, value = param
            if name == 'alpha':
                model.set_param_hint(name, value=value, vary=True, min=0, max=1)
            else:
                model.set_param_hint(name, value=value, vary=True, min=0)

        params = model.make_params()
        result = model.fit(self.fit_data[:self.training_period], params, method="leastsq", x=x)  # fitting
        params = result.best_values
        print(params)
        print("RO: " + str(self.R0))

        self.update_params(params)
        self.best_fit = result.best_fit

    def fine_tune(self, fit_fun, params_to_vary, data):
        self.days = len(self.timeline[:self.training_period])
        x = np.linspace(0, self.days - 1, self.days, dtype=int)  # x_data is just [0, 1, ..., max_days] array

        model = lmfit.Model(fit_fun)
        # we set the parameters (and some initial parameter guesses)
        for param in self.params:
            name, value = param
            if name in params_to_vary:
                if name == 'alpha':
                    model.set_param_hint(name, value=value, vary=True, min=0, max=1)
                else:
                    model.set_param_hint(name, value=value, vary=True, min=0)
            else:
                model.set_param_hint(name, value=value, vary=False, min=0)

        params = model.make_params()
        result = model.fit(data[:self.training_period], params, method="leastsq", x=x)  # fitting
        params = result.best_values
        print(params)
        print("RO: " + str(self.R0))

        self.update_params(params)
        self.best_fit = result.best_fit

    def predict(self):
        raise NotImplemented

    def train_and_predict(self):
        self.train()
        return self.predict()

    def fine_tune_and_predict(self):
        raise NotImplemented


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

    def fit_fun(self, x, beta, gamma, i0):
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
    def __init__(self, db, virus_name, country_name, predcit_range, training_period=-1, state_name=None):
        data = db.get_epidemic_data_in([country_name], ['deaths', 'confirmed', 'recovered'], virus_name,
                                       since_epidemy_start=True, state_name=state_name)

        timeline = data['dates']
        extended_timeline = timeline

        self.confirmed = data[country_name]['confirmed']
        self.recovered = data[country_name]['recovered']
        self.deaths = data[country_name]['deaths']

        self.active = [data[country_name]['confirmed'][i] - self.recovered[i] - self.deaths[i] for i in
                       range(len(self.deaths))]

        N = data[country_name]['population']

        self.beta = 0.2
        self.gamma = 0.1

        self.I0 = 1
        self.R0 = self.beta / self.gamma
        self.S0 = N - self.I0 - self.R0
        self.Y0 = self.S0, self.I0, self.R0

        params = [
            ("beta", self.beta),
            ("gamma", self.gamma),
            ("i0", self.I0),
        ]

        super().__init__(db, predcit_range, training_period, timeline, extended_timeline, N, params, self.active)

    @staticmethod
    def deriv(y, t, N, beta, gamma):
        S, I, R = y
        dSdt = -beta * S * I / N
        dIdt = beta * S * I / N - gamma * I
        dRdt = gamma * I
        return dSdt, dIdt, dRdt

    def fit_fun(self, x, beta, gamma, i0):
        r0 = beta / gamma
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
        self.R0 = self.beta / self.gamma

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
    def __init__(self, db, virus_name, country_name, predcit_range, training_period=-1, beta=2.2, gamma=0.85, i0=655,
                 state_name=None):
        data = db.get_epidemic_data_in([country_name], ['deaths', 'confirmed', 'recovered'], virus_name,
                                       since_epidemy_start=True, state_name=state_name)

        timeline = data['dates']
        extended_timeline = timeline

        self.confirmed = data[country_name]['confirmed']
        self.recovered = data[country_name]['recovered']
        self.deaths = data[country_name]['deaths']

        self.active = [data[country_name]['confirmed'][i] - self.recovered[i] - self.deaths[i] for i in
                       range(len(self.deaths))]

        N = data[country_name]['population']

        self.beta = beta
        self.gamma = gamma
        self.delta = 0.2

        self.I0 = i0
        self.R0 = self.beta / self.gamma
        self.S0 = N - self.I0 - self.R0
        self.E0 = N - (self.S0 + self.I0 + self.R0)
        self.Y0 = self.S0, self.E0, self.I0, self.R0

        params = [
            ("beta", self.beta),
            ("gamma", self.gamma),
            ("delta", self.delta),
            ("i0", self.I0),
        ]

        super().__init__(db, predcit_range, training_period, timeline, extended_timeline, N, params, self.active)

    @staticmethod
    def deriv(y, t, N, beta, gamma, delta):
        S, E, I, R = y

        dSdt = -beta * S * I / N
        dEdt = beta * S * I / N - delta * E
        dIdt = delta * E - gamma * I
        dRdt = gamma * I

        return dSdt, dEdt, dIdt, dRdt

    def fit_fun(self, x, beta, gamma, delta, i0):
        t = np.linspace(0, self.days, self.days)
        r0 = beta / gamma
        s0 = self.N - i0 - r0
        e0 = self.N - (s0 + i0 + r0)

        y0 = s0, e0, i0, r0
        ret = odeint(self.deriv, y0, t, args=(self.N, beta, gamma, delta))
        S, E, I, R = ret.T
        return I[x]

    def fit_E_fun(self, x, beta, gamma, delta, i0):
        t = np.linspace(0, self.days, self.days)
        r0 = beta / gamma
        s0 = self.N - i0 - r0
        e0 = self.N - (s0 + i0 + r0)

        y0 = s0, e0, i0, r0
        ret = odeint(self.deriv, y0, t, args=(self.N, beta, gamma, delta))
        S, E, I, R = ret.T
        return E[x]

    def update_params(self, params):
        self.beta = params['beta']
        self.gamma = params['gamma']
        self.delta = params['delta']
        self.I0 = params['i0']
        self.R0 = self.beta / self.gamma

        self.S0 = self.N - self.I0 - self.R0
        self.E0 = self.N - (self.S0 + self.I0 + self.R0)
        self.Y0 = self.S0, self.E0, self.I0, self.R0

    def predict(self):
        self.extended_timeline = self.extend_timeline(self.timeline, self.predict_range)
        size = len(self.extended_timeline)
        t = np.linspace(0, size, size)
        ret = odeint(self.deriv, self.Y0, t, args=(self.N, self.beta, self.gamma, self.delta))
        S, E, I, R = ret.T
        return S, E, I, R

    def fine_tune_and_predict(self):
        print("-- Fine tuning delta --")
        self.fine_tune(self.fit_E_fun, ['delta'], self.active)
        return self.predict()


class SEIRD(EpidemicModel):  # from S to E and possibly to I then R and never back - immunity
    def __init__(self, db, virus_name, country_name, predcit_range, training_period=-1, beta=2.2, gamma=0.85, delta=0.3,
                 i0=655, state_name=None):
        data = db.get_epidemic_data_in([country_name], ['deaths', 'confirmed', 'recovered'], virus_name,
                                       since_epidemy_start=True, state_name=state_name)

        timeline = data['dates']
        extended_timeline = timeline

        self.confirmed = data[country_name]['confirmed']
        self.recovered = data[country_name]['recovered']
        self.deaths = data[country_name]['deaths']

        self.active = [data[country_name]['confirmed'][i] - self.recovered[i] - self.deaths[i] for i in
                       range(len(self.deaths))]

        N = data[country_name]['population']

        self.beta = beta
        self.gamma = gamma
        self.delta = delta
        self.alpha = 0.05
        self.rho = 1 / 9

        self.I0 = i0
        self.R0 = self.beta / self.gamma
        self.S0 = N - self.I0 - self.R0
        self.E0 = N - (self.S0 + self.I0 + self.R0)
        self.D0 = self.deaths[0]
        self.Y0 = self.S0, self.E0, self.I0, self.R0, self.D0

        params = [
            ("beta", self.beta),
            ("gamma", self.gamma),
            ("delta", self.delta),
            ("alpha", self.alpha),
            ("rho", self.rho),
            ("i0", self.I0),
        ]

        super().__init__(db, predcit_range, training_period, timeline, extended_timeline, N, params, self.active)

    @staticmethod
    def deriv(y, t, N, beta, gamma, delta, alpha, rho):
        S, E, I, R, D = y

        dSdt = -beta * S * I / N
        dEdt = beta * S * I / N - delta * E
        dIdt = delta * E - (1 - alpha) * gamma * I - alpha * rho * I

        dRdt = (1 - alpha) * gamma * I
        dDdt = alpha * rho * I

        return dSdt, dEdt, dIdt, dRdt, dDdt

    def fit_fun(self, x, beta, gamma, delta, alpha, rho, i0):
        r0 = beta / gamma
        t = np.linspace(0, self.days, self.days)
        s0 = self.N - i0 - r0
        e0 = self.N - (s0 + i0 + r0)

        y0 = s0, e0, i0, r0, self.D0
        ret = odeint(self.deriv, y0, t, args=(self.N, beta, gamma, delta, alpha, rho))
        S, E, I, R, D = ret.T
        return I[x]

    def fit_E_fun(self, x, beta, gamma, delta, alpha, rho, i0):
        r0 = beta / gamma
        t = np.linspace(0, self.days, self.days)
        s0 = self.N - i0 - r0
        e0 = self.N - (s0 + i0 + r0)

        y0 = s0, e0, i0, r0, self.D0
        ret = odeint(self.deriv, y0, t, args=(self.N, beta, gamma, delta, alpha, rho))
        S, E, I, R, D = ret.T
        return E[x]

    def fit_D_fun(self, x, beta, gamma, delta, alpha, rho, i0):
        r0 = beta / gamma
        t = np.linspace(0, self.days, self.days)
        s0 = self.N - i0 - r0
        e0 = self.N - (s0 + i0 + r0)

        y0 = s0, e0, i0, r0, self.D0
        ret = odeint(self.deriv, y0, t, args=(self.N, beta, gamma, delta, alpha, rho))
        S, E, I, R, D = ret.T
        return D[x]

    def update_params(self, params):
        self.beta = params['beta']
        self.gamma = params['gamma']
        self.delta = params['delta']
        self.alpha = params['alpha']
        self.rho = params['rho']
        self.I0 = params['i0']
        self.R0 = self.beta / self.gamma

        self.S0 = self.N - self.I0 - self.R0
        self.E0 = self.N - (self.S0 + self.I0 + self.R0)
        self.Y0 = self.S0, self.E0, self.I0, self.R0, self.D0

    def predict(self):
        self.extended_timeline = self.extend_timeline(self.timeline, self.predict_range)
        size = len(self.extended_timeline)
        t = np.linspace(0, size, size)
        ret = odeint(self.deriv, self.Y0, t, args=(self.N, self.beta, self.gamma, self.delta, self.alpha, self.rho))
        S, E, I, R, D = ret.T
        return S, E, I, R, D

    def fine_tune_and_predict(self):
        # print("-- Fine tuning delta --")
        # self.fine_tune(self.fit_E_fun, ['delta'], self.active)
        print("-- Fine tuning alpha, rho --")
        self.fine_tune(self.fit_D_fun, ['alpha', 'rho'], self.deaths)
        return self.predict()


class TwitterModel:
    def __init__(self, db, predict_range, country_name, state_name=None):
        pandemic_data = db.get_epidemic_data_in([country_name], ['deaths', 'confirmed', 'recovered'], "COVID19",
                                                since_epidemy_start=False, state_name=state_name,
                                                from_date='2020-03-07')
        country_code = coco.convert(country_name, to='ISO2').lower()

        self.timeline = pandemic_data['dates']
        self.extended_timeline = self.timeline

        self.predict_range = predict_range

        self.tweets, self.positive_tweets, self.negative_tweets = db.get_tweets_per_day_in(country_code, state_name,
                                                                                           self.timeline[0],
                                                                                           self.timeline[-1])

        self.users_in_country = db.get_number_of_users_in(country_code, state_name)

        self.days = len(self.tweets)

        self.confirmed = pandemic_data[country_name]['confirmed']
        self.recovered = pandemic_data[country_name]['recovered']
        self.deaths = pandemic_data[country_name]['deaths']

        self.active = [pandemic_data[country_name]['confirmed'][i] - self.recovered[i] - self.deaths[i] for i in
                       range(len(self.deaths))]

        self.N = pandemic_data[country_name]['population']

        self.beta = 1
        self.gamma = 1
        self.delta = 1

        self.t_infl = 1 / self.users_in_country
        self.t_outdt = 5  # tweet outdating rate
        self.t_s = self.users_in_country / self.N
        self.t_e = self.users_in_country / self.N
        self.t_i = self.users_in_country / self.N

        self.tweets_line = self.create_tweets_line()

        self.I0 = 1
        self.R0 = self.beta / self.gamma
        self.S0 = self.N - self.I0 - self.R0
        self.E0 = self.N - (self.S0 + self.I0 + self.R0)
        self.Y0 = self.S0, self.E0, self.I0, self.R0
        self.T0 = self.tweets[0]

        self.params = [
            ("beta", self.beta),
            ("gamma", self.gamma),
            ("delta", self.delta),
            ("i0", self.I0),
            ("t0", self.T0),
            ("t_infl", self.t_infl),
            ("t_outdt", self.t_outdt),
            ("t_s", self.t_s),
            ("t_e", self.t_e),
            ("t_i", self.t_i)
        ]
        self.frozen_params = ["beta", "gamma", "delta", "i0", "t0", "t_outdt"]

        self.fit_data = self.active
        self.fit_fun = self.fit_I

    def initialize(self, beta, gamma, delta, i0):
        self.beta = beta
        self.gamma = gamma
        self.delta = delta
        self.I0 = i0
        self.R0 = self.beta / self.gamma

        self.S0 = self.N - self.I0 - self.R0
        self.E0 = self.N - (self.S0 + self.I0 + self.R0)
        self.Y0 = self.S0, self.E0, self.I0, self.R0, self.T0

        self.params = [
            ("beta", self.beta),
            ("gamma", self.gamma),
            ("delta", self.delta),
            ("i0", self.I0),
            ("t0", self.T0),
            ("t_infl", self.t_infl),
            ("t_outdt", self.t_outdt),
            ("t_s", self.t_s),
            ("t_e", self.t_e),
            ("t_i", self.t_i)
        ]

    def create_tweets_line(self):
        line = []
        day_number = 0
        tw = 0
        out_date = int(self.t_outdt)
        for d_tweets in self.tweets:
            tw += d_tweets
            if day_number >= out_date:
                tw -= self.tweets[day_number - out_date]
            line.append(tw)
            day_number += 1
        return line

    @staticmethod
    def deriv(y, t, N, beta, gamma, delta, t_infl, t_outdt, t_s, t_e, t_i):
        S, E, I, R, T = y

        dSdt = -beta * S * I / N * exp(-t_infl * T)
        dEdt = beta * S * I / N * exp(-t_infl * T) - delta * E
        dIdt = delta * E - gamma * I
        dRdt = gamma * I
        dTdt = t_s * S + t_e * E + t_i * I - t_outdt * T

        return dSdt, dEdt, dIdt, dRdt, dTdt

    def fit_I(self, x, beta, gamma, delta, i0, t0, t_infl, t_outdt, t_s, t_e, t_i):
        t = np.linspace(0, self.days, self.days)
        r0 = beta / gamma
        s0 = self.N - i0 - r0
        e0 = self.N - (s0 + i0 + r0)

        y0 = s0, e0, i0, r0, t0
        ret = odeint(self.deriv, y0, t, args=(self.N, beta, gamma, delta, t_infl, t_outdt, t_s, t_e, t_i))
        S, E, I, R, T = ret.T
        return I[x]

    def fit_T(self, x, beta, gamma, delta, i0, t0, t_infl, t_outdt, t_s, t_e, t_i):
        t = np.linspace(0, self.days, self.days)
        r0 = beta / gamma
        s0 = self.N - i0 - r0
        e0 = self.N - (s0 + i0 + r0)

        y0 = s0, e0, i0, r0, t0
        ret = odeint(self.deriv, y0, t, args=(self.N, beta, gamma, delta, t_infl, t_outdt, t_s, t_e, t_i))
        S, E, I, R, T = ret.T
        return T[x]

    def update_params(self, params):
        self.beta = params['beta']
        self.gamma = params['gamma']
        self.delta = params['delta']
        self.I0 = params['i0']
        self.R0 = self.beta / self.gamma

        self.t_s = params['t_s']
        self.t_e = params['t_e']
        self.t_i = params['t_i']

        self.t_infl = params['t_infl']
        self.t_outdt = params['t_outdt']

        self.S0 = self.N - self.I0 - self.R0
        self.E0 = self.N - (self.S0 + self.I0 + self.R0)
        self.Y0 = self.S0, self.E0, self.I0, self.R0, self.T0

    @staticmethod
    def extend_timeline(dates: list, time_range: int):
        last_date = dates[-1]
        last_date = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S").date()
        for i in range(time_range):
            next_date = last_date + timedelta(days=1)
            dates.append(next_date.strftime("%Y-%m-%d %H:%M:%S"))
            last_date = next_date
        return dates

    def train(self):
        x = np.linspace(0, self.days - 1, self.days, dtype=int)  # x_data is just [0, 1, ..., max_days] array

        model = lmfit.Model(self.fit_fun)
        # we set the parameters (and some initial parameter guesses)
        for param in self.params:
            name, value = param
            if name in self.frozen_params:
                model.set_param_hint(name, value=value, vary=True, min=value - 1, max=value + 1)
            else:
                model.set_param_hint(name, value=value, vary=True, min=0)

        params = model.make_params()
        result = model.fit(self.fit_data[:self.days], params, method="leastsq", x=x)  # fitting
        params = result.best_values
        print(params)
        print("RO: " + str(self.R0))

        self.update_params(params)
        self.best_fit = result.best_fit
        self.tweets_line = self.create_tweets_line()

    def predict(self):
        self.extended_timeline = self.extend_timeline(self.timeline, self.predict_range)
        size = len(self.extended_timeline)
        t = np.linspace(0, size, size)
        ret = odeint(self.deriv, self.Y0, t, args=(
            self.N, self.beta, self.gamma, self.delta, self.t_infl, self.t_outdt, self.t_s, self.t_e, self.t_i))
        S, E, I, R, T = ret.T
        return S, E, I, R, T

    def train_and_predict(self, fine_tune=False):
        self.train()
        if fine_tune:
            self.frozen_params += ["t_infl", "t_s", "t_e", "t_i"]
            self.fit_data = self.tweets_line
            self.fit_fun = self.fit_T
            self.train()
        return self.predict()


class TwitterSentimentModel:
    def __init__(self, db, predict_range, country_name, state_name=None):
        pandemic_data = db.get_epidemic_data_in([country_name], ['deaths', 'confirmed', 'recovered'], "COVID19",
                                                since_epidemy_start=False, state_name=state_name,
                                                from_date='2020-03-07')
        country_code = coco.convert(country_name, to='ISO2').lower()

        self.timeline = pandemic_data['dates']
        self.extended_timeline = self.timeline

        self.predict_range = predict_range

        self.tweets, self.positive_tweets, self.negative_tweets = db.get_tweets_per_day_in(country_code, state_name,
                                                                                           self.timeline[0],
                                                                                           self.timeline[-1])

        self.users_in_country = db.get_number_of_users_in(country_code, state_name)

        self.days = len(self.tweets)

        self.confirmed = pandemic_data[country_name]['confirmed']
        self.recovered = pandemic_data[country_name]['recovered']
        self.deaths = pandemic_data[country_name]['deaths']

        self.active = [pandemic_data[country_name]['confirmed'][i] - self.recovered[i] - self.deaths[i] for i in
                       range(len(self.deaths))]

        self.N = pandemic_data[country_name]['population']

        self.beta = 1
        self.gamma = 1
        self.delta = 1

        self.tp_infl = 1 / self.users_in_country # reduces infections
        self.t_outdt = 5  # tweet outdating rate
        self.tp_s = self.users_in_country / self.N
        self.tp_e = self.users_in_country / self.N
        self.tp_i = self.users_in_country / self.N

        self.tn_infl = 1 / self.users_in_country # increases infections
        self.tn_s = self.users_in_country / self.N
        self.tn_e = self.users_in_country / self.N
        self.tn_i = self.users_in_country / self.N

        self.tweets_line = self.create_tweets_line()

        self.I0 = 1
        self.R0 = self.beta / self.gamma
        self.S0 = self.N - self.I0 - self.R0
        self.E0 = self.N - (self.S0 + self.I0 + self.R0)
        self.Y0 = self.S0, self.E0, self.I0, self.R0
        self.TP0 = self.positive_tweets[0]
        self.TN0 = self.negative_tweets[0]

        self.params = [
            ("beta", self.beta),
            ("gamma", self.gamma),
            ("delta", self.delta),
            ("i0", self.I0),
            ("tp0", self.TP0),
            ("tn0", self.TN0),
            ("tp_infl", self.tp_infl),
            ("tn_infl", self.tn_infl),
            ("t_outdt", self.t_outdt),
            ("tp_s", self.tp_s),
            ("tp_e", self.tp_e),
            ("tp_i", self.tp_i),
            ("tn_s", self.tn_s),
            ("tn_e", self.tn_e),
            ("tn_i", self.tn_i)
        ]
        self.frozen_params = ["beta", "gamma", "delta", "i0", "tp0", "tn0", "t_outdt"]

        self.fit_data = self.active
        self.fit_fun = self.fit_I

    def initialize(self, beta, gamma, delta, i0):
        self.beta = beta
        self.gamma = gamma
        self.delta = delta
        self.I0 = i0
        self.R0 = self.beta / self.gamma

        self.S0 = self.N - self.I0 - self.R0
        self.E0 = self.N - (self.S0 + self.I0 + self.R0)
        self.Y0 = self.S0, self.E0, self.I0, self.R0, self.TP0, self.TN0

        self.params = [
            ("beta", self.beta),
            ("gamma", self.gamma),
            ("delta", self.delta),
            ("i0", self.I0),
            ("tp0", self.TP0),
            ("tn0", self.TN0),
            ("tp_infl", self.tp_infl),
            ("tn_infl", self.tn_infl),
            ("t_outdt", self.t_outdt),
            ("tp_s", self.tp_s),
            ("tp_e", self.tp_e),
            ("tp_i", self.tp_i),
            ("tn_s", self.tn_s),
            ("tn_e", self.tn_e),
            ("tn_i", self.tn_i)
        ]

    def create_tweets_line(self):
        line = []
        day_number = 0
        tw = 0
        out_date = int(self.t_outdt)
        for d_tweets in self.tweets:
            tw += d_tweets
            if day_number >= out_date:
                tw -= self.tweets[day_number - out_date]
            line.append(tw)
            day_number += 1
        return line

    @staticmethod
    def deriv(y, t, N, beta, gamma, delta, tp_infl, tn_infl, t_outdt, tp_s, tp_e, tp_i, tn_s, tn_e, tn_i):
        S, E, I, R, TP, TN = y

        dSdt = -beta * S * I / N * exp(-tp_infl * TP + tn_infl * TN)
        dEdt = beta * S * I / N * exp(-tp_infl * TP + tn_infl * TN) - delta * E
        dIdt = delta * E - gamma * I
        dRdt = gamma * I
        dTPdt = tp_s * S + tp_e * E + tp_i * I - t_outdt * TP
        dTNdt = tn_s * S + tn_e * E + tn_i * I - t_outdt * TN

        return dSdt, dEdt, dIdt, dRdt, dTPdt, dTNdt

    def fit_I(self, x, beta, gamma, delta, i0, tp0, tn0, tp_infl, tn_infl, t_outdt, tp_s, tp_e, tp_i, tn_s, tn_e, tn_i):
        t = np.linspace(0, self.days, self.days)
        r0 = beta / gamma
        s0 = self.N - i0 - r0
        e0 = self.N - (s0 + i0 + r0)

        y0 = s0, e0, i0, r0, tp0, tn0
        ret = odeint(self.deriv, y0, t, args=(self.N, beta, gamma, delta, tp_infl, tn_infl, t_outdt, tp_s, tp_e, tp_i, tn_s, tn_e, tn_i))
        S, E, I, R, TP, TN = ret.T
        return I[x]

    def fit_TP(self, x, beta, gamma, delta, i0, tp0, tn0, tp_infl, tn_infl, t_outdt, tp_s, tp_e, tp_i, tn_s, tn_e, tn_i):
        t = np.linspace(0, self.days, self.days)
        r0 = beta / gamma
        s0 = self.N - i0 - r0
        e0 = self.N - (s0 + i0 + r0)

        y0 = s0, e0, i0, r0, tp0, tn0
        ret = odeint(self.deriv, y0, t, args=(self.N, beta, gamma, delta, tp_infl, tn_infl, t_outdt, tp_s, tp_e, tp_i, tn_s, tn_e, tn_i))
        S, E, I, R, TP, TN = ret.T
        return TP[x]

    def fit_TN(self, x, beta, gamma, delta, i0, tp0, tn0, tp_infl, tn_infl, t_outdt, tp_s, tp_e, tp_i, tn_s, tn_e,
               tn_i):
        t = np.linspace(0, self.days, self.days)
        r0 = beta / gamma
        s0 = self.N - i0 - r0
        e0 = self.N - (s0 + i0 + r0)

        y0 = s0, e0, i0, r0, tp0, tn0
        ret = odeint(self.deriv, y0, t,
                     args=(self.N, beta, gamma, delta, tp_infl, tn_infl, t_outdt, tp_s, tp_e, tp_i, tn_s, tn_e, tn_i))
        S, E, I, R, TP, TN = ret.T
        return TN[x]

    def update_params(self, params):
        self.beta = params['beta']
        self.gamma = params['gamma']
        self.delta = params['delta']
        self.I0 = params['i0']
        self.R0 = self.beta / self.gamma

        self.tp_s = params['tp_s']
        self.tp_e = params['tp_e']
        self.tp_i = params['tp_i']

        self.tn_s = params['tn_s']
        self.tn_e = params['tn_e']
        self.tn_i = params['tn_i']

        self.tp_infl = params['tp_infl']
        self.tn_infl = params['tn_infl']
        self.t_outdt = params['t_outdt']

        self.S0 = self.N - self.I0 - self.R0
        self.E0 = self.N - (self.S0 + self.I0 + self.R0)
        self.Y0 = self.S0, self.E0, self.I0, self.R0, self.TP0, self.TN0

    @staticmethod
    def extend_timeline(dates: list, time_range: int):
        last_date = dates[-1]
        last_date = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S").date()
        for i in range(time_range):
            next_date = last_date + timedelta(days=1)
            dates.append(next_date.strftime("%Y-%m-%d %H:%M:%S"))
            last_date = next_date
        return dates

    def train(self):
        x = np.linspace(0, self.days - 1, self.days, dtype=int)  # x_data is just [0, 1, ..., max_days] array

        model = lmfit.Model(self.fit_fun)
        # we set the parameters (and some initial parameter guesses)
        for param in self.params:
            name, value = param
            if name in self.frozen_params:
                model.set_param_hint(name, value=value, vary=True, min=value - 1, max=value + 1)
            else:
                model.set_param_hint(name, value=value, vary=True, min=0)

        params = model.make_params()
        result = model.fit(self.fit_data[:self.days], params, method="leastsq", x=x)  # fitting
        params = result.best_values
        print(params)
        print("RO: " + str(self.R0))

        self.update_params(params)
        self.best_fit = result.best_fit
        self.tweets_line = self.create_tweets_line()

    def predict(self):
        self.extended_timeline = self.extend_timeline(self.timeline, self.predict_range)
        size = len(self.extended_timeline)
        t = np.linspace(0, size, size)
        ret = odeint(self.deriv, self.Y0, t, args=(
            self.N, self.beta, self.gamma, self.delta, self.tp_infl, self.tn_infl, self.t_outdt,
            self.tp_s, self.tp_e, self.tp_i, self.tn_s, self.tn_e, self.tn_i))
        S, E, I, R, TP, TN = ret.T
        return S, E, I, R, TP, TN

    def train_and_predict(self, fine_tune=False):
        self.train()
        if fine_tune:
            self.frozen_params += ["tp_infl", "tn_infl", "tp_s", "tp_e", "tp_i", "tn_s", "tn_e", "tn_i"]
            self.fit_data = self.tweets_line
            self.fit_fun = self.fit_TP
            self.fit_fun = self.fit_TN
            self.train()
        return self.predict()

