'''

S - susceptibles, N - (I+R)
I - infectious
R - recovered with immunity
E - exposed, infected but not infectious, N - (S+I+R)
D - deceased
M - newborn immune for the first few months

gamma - recover rate
beta - infection rate
mi - mortality rate
lambda - birth rate
alpha - incubation period
delta -
assumption - birth rate = death rate, mi = lambda, N is constant

'''


class SIS: # from S to I and if recovered back to S

    def __init__(self, S, I, N, gamma, beta):

        self.dSdt = -(beta * S * I)/N + gamma * I
        self.dIdt = -(beta * S * I)/N - gamma * I


class SIR: # from I to R and never back to S

    def __init__(self, S, I, N, gamma, beta):

        self.dSdt = -(beta * S * I)/N
        self.dIdt = (beta * S * I)/N - gamma * I
        self.dRdt = gamma * I


class SEIR: # from S to E and possibly to I then R and never back - immunity

    def __init__(self, S, E, I, R, N, gamma, beta, mi, alpha):

        self.E = N - (S+I+R)

        self.dSdt = mi * N - mi * S - beta * (I/N) * S
        self.dEdt = beta * (I/N) * S - (mi + alpha)*E
        self.dIdt = alpha*E - (gamma+mi)*I
        self.dRdt = gamma*I - mi*R

class SEIS: # like SEIR but without immunity

    def __init__(self, S, E, I, N, gamma, beta, mi, alpha):
        self.dSdt = mi * N - mi * S - beta * (I / N) * S
        self.dEdt = beta * (I / N) * S - (mi + alpha) * E
        self.dIdt = alpha * E - (gamma + mi) * I


class SIRD: # like SIR + mi but differentiates between deceased and recovered

    def __init__(self, S, I, N, gamma, beta, mi):

        self.dSdt = -(beta * S * I)/N
        self.dIdt = (beta * S * I)/N - gamma * I - mi * I
        self.dRdt = gamma * I
        self.dDdt = mi * I
