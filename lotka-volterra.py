import numpy as np
import matplotlib as plt
import scipy
import pylab 
import argparse

""""
    This is the simulation of the predator-prey model (Lotka-Volterra) in a 2D phase 
    space. We follow the notational convention that can be found on this wikipedia
    page: https://en.wikipedia.org/wiki/Lotka%E2%80%93Volterra_equations

    As shown on Wikipedia, we consider two first-order Ordinary Differential Equations
    given by:
        - dx/dt = \alpha * x - \beta * xy
        - dy/dt = \delta * xy - \gamma * y

    where \alpha, \beta, \delta and \gamma are the parameters describing the interaction
    of the two species x (rabbits) and y (wolves). 
"""


def get_growth_rates(init_cond, alpha, beta, gamma, delta):
    prey_growth_rate = alpha * init_cond[0] - beta * init_cond[0] * init_cond[1]
    pred_growth_rate = delta * init_cond[0] * init_cond[1] - gamma * init_cond[1]
    return np.array([prey_growth_rate, pred_growth_rate])


def main(args):
    #retrieve model parameters
    alpha = args.alpha
    beta = args.beta
    gamma = args.gamma
    delta = args.delta
    initial_condition = np.array([args.init_prey, args.init_pred])


    print('Model Parameters => alpha = {}, beta = {}, gamma = {}, delta = {}'.\
                                            format(alpha, beta, gamma, delta))

    """ Plotting the evolution of the rabit and wolves populations"""


if __name__=='__main__':
    parser = argparse.ArgumentParser(description='simulation of the Lotka-Volterra model in a 2D phase space')
    parser.add_argument('--alpha', type=float, default=1.0,
                                help='alpha parameter in the first equation')
    parser.add_argument('--beta', type=float, default=0.1,
                                help='beta parameter in the first equation')
    parser.add_argument('--delta', type=float, default=1.5,
                                help='delta parameter in the second equaation')
    parser.add_argument('--gamma', type=float, default=0.75,
                                help='gamma parameter in the second equation')
    parser.add_argument('--init_prey', type=int, default=10,
                                help='initial population of the prey(default=10)')
    parser.add_argument('--init_pred', type=int, default=5,
                                help='initial population of the predator(default=5)')

    args = parser.parse_args()
    main(args)