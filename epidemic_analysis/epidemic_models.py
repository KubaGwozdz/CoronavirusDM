import numpy as np
from scipy.integrate import odeint, solve_ivp
from scipy.optimize import minimize
from functools import partial
from datetime import datetime, timedelta

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


class SIR(EpidemicModel):  # from I to R and never back to S
    def __init__(self, s_0, i_0, r_0, country_name, db, virus_name, predict_range, gamma=None, beta=None):
        # self.dSdt = -(beta * S * I)/N
        # self.dIdt = (beta * S * I)/N - gamma * I
        # self.dRdt = gamma * I

        super().__init__(db, predict_range)
        # self.derivs = [self.dSdt, self.dIdt, self.dRdt]

        data = db.get_epidemic_data_in([country_name], ['deaths', 'confirmed', 'recovered'], virus_name)
        self.timeline = data['dates']
        self.recovered = data[country_name]['recovered']
        self.deaths = data[country_name]['deaths']
        self.active = [data[country_name]['confirmed'][i] - self.recovered[i] - self.deaths[i] for i in range(len(self.deaths))]
        self.s_0 = s_0
        self.i_0 = i_0
        self.r_0 = r_0
        self.beta = beta
        self.gamma = gamma

    @staticmethod
    def model(beta, gamma, t, y):
        S = y[0]
        I = y[1]
        R = y[2]
        return [-beta * S * I, beta * S * I - gamma * I, gamma * I]

    def loss(self, point, active, recovered, s_0, i_0, r_0):
        size = len(active)
        beta, gamma = point

        SIR = partial(self.model, beta, gamma)

        solution = solve_ivp(SIR, [0, size], [s_0, i_0, r_0], t_eval=np.arange(0, size, 1), vectorized=True)
        l1 = np.sqrt(np.mean((solution.y[1] - active) ** 2))
        l2 = np.sqrt(np.mean((solution.y[2] - recovered) ** 2))
        alpha = 0.1
        return alpha * l1 + (1 - alpha) * l2

    def predict(self):
        self.timeline = self.extend_timeline(self.timeline, self.predict_range)
        size = len(self.timeline)
        SIR = partial(self.model, self.beta, self.gamma)

        # extended_active = np.concatenate((data.values, [None] * (size - len(data.values))))
        # extended_recovered = np.concatenate((recovered.values, [None] * (size - len(recovered.values))))
        # extended_death = np.concatenate((death.values, [None] * (size - len(death.values))))

        prediction = solve_ivp(SIR, [0, size], [self.s_0, self.i_0, self.r_0], t_eval=np.arange(0, size, 1))
        susceptible = prediction.y[0]
        infected = prediction.y[1]
        recovered = prediction.y[2]
        return prediction, susceptible, infected, recovered

    def train(self):
        optimal = minimize(self.loss, [0.001, 0.001], args=(self.active, self.recovered, self.s_0, self.i_0, self.r_0),
                           method='L-BFGS-B', bounds=[(0.00000001, 0.4), (0.00000001, 0.4)])
        print(optimal)
        beta, gamma = optimal.x
        self.beta = beta
        self.gamma = gamma

    def train_and_predict(self):
        self.train()
        return self.predict()


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
