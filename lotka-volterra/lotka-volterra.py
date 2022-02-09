from cProfile import label
import numpy as np
import matplotlib as plt
from scipy import integrate
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



def plot_phase_curve(callable, alpha, beta, gamma, delta, time):
    values = np.linspace(0.2, 0.9, 4)
    colors = pylab.cm.autumn_r(np.linspace(0.3, 1., len(values)))

    figure = pylab.figure()
    for value in values:
        initial_position = value * np.array([gamma/delta, alpha/beta])
        solution, _ = integrate.odeint(callable, initial_position,time,
                                full_output=True)
        pylab.plot(
            solution[:,0], solution[:,1], lw=3.5*value
        )
    """ compute vector at each point to get the vector field"""
    x_axis = np.linspace(0, pylab.xlim(xmin=0)[1], 20)
    y_axis = np.linspace(0, pylab.ylim(ymin=0)[1], 20)

    grid_x, grid_y = np.meshgrid(x_axis, y_axis)
    growth_rate_x, growth_rate_y = callable([grid_x, grid_y])

    """ plotting the phase curve"""
    pylab.title('Phase Curve')
    pylab.quiver(
        grid_x, grid_y, growth_rate_x, growth_rate_y, pivot='mid'
        )
    pylab.xlabel('Prey Population')
    pylab.ylabel('Predator Population')
    pylab.grid()
    pylab.xlim(0, pylab.xlim(xmin=0)[1])
    pylab.ylim(0, pylab.ylim(ymin=0)[1])
    figure.savefig('results/phase_curve.png')



def main(args):
    """ retrieve model parameters """
    alpha = args.alpha
    beta = args.beta
    gamma = args.gamma
    delta = args.delta
    initial_condition = np.array([args.init_prey, args.init_pred])


    print('Model Parameters => alpha = {}, beta = {}, gamma = {}, delta = {}'.\
                                            format(alpha, beta, gamma, delta))

    def get_growth_rates(initial_condition, time=0):
        prey_growth_rate = alpha * initial_condition[0] - beta * initial_condition[0] * initial_condition[1]
        pred_growth_rate = delta * initial_condition[0] * initial_condition[1] - gamma * initial_condition[1]
        return np.array([prey_growth_rate, pred_growth_rate])

    """ Solve the ODE"""
    time = np.linspace(0, 20, 1000)
    solution, message = integrate.odeint(get_growth_rates, initial_condition,
                                                time, full_output=True)

    """ Plotting the evolution of the rabit and wolves populations"""
    prey, predator = solution.T
    figure1 = pylab.figure()
    pylab.plot(time, prey, '-g', label='prey')
    pylab.plot(time, predator, '-b', label = 'predator')
    pylab.grid()
    pylab.legend(loc='best')
 
    pylab.xlabel('Time')
    pylab.ylabel('Population Growth')
    pylab.title('Predator and Prey Dynamics')
    figure1.savefig('results/fig1.png')

    """ Plotting the phase curve with the vector field"""
    plot_phase_curve(get_growth_rates, alpha, beta, gamma, delta, time)
    


if __name__=='__main__':
    parser = argparse.ArgumentParser(description='simulation of the Lotka-Volterra model in a 2D phase space')
    parser.add_argument('--alpha', type=float, default=1.0,
                                help='alpha parameter in the first equation')
    parser.add_argument('--beta', type=float, default=0.1,
                                help='beta parameter in the first equation')
    parser.add_argument('--delta', type=float, default=0.75,
                                help='delta parameter in the second equaation')
    parser.add_argument('--gamma', type=float, default=1.5,
                                help='gamma parameter in the second equation')
    parser.add_argument('--init_prey', type=int, default=10,
                                help='initial population of the prey(default=10)')
    parser.add_argument('--init_pred', type=int, default=5,
                                help='initial population of the predator(default=5)')

    args = parser.parse_args()
    main(args)