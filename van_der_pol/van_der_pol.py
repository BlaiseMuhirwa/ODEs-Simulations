
from pydoc import Helper
import numpy as np
import pandas as pd
from scipy.integrate import solve_ivp
import argparse
import os
import matplotlib.pyplot as plt
import warnings

warnings.filterwarnings('ignore')
plt.rcParams["figure.figsize"] = (15,15)


"""
This is a simulation of Van der Pol's equation given by
    d^2x/dt^2 - mu(1-x^2)dx/dt + x = 0

author=Blaise Munyampirwa

"""


path = os.getcwd()
def main(args):
    def van_del_pol_ode(time, a):
        x, dxdt = a
        return np.array(
            [dxdt, mu*(1-x**2)*dxdt - x]
        )
    integration_start = 0
    integration_end = 10
    time = np.linspace(integration_start, integration_end, 500)

    mu_array = args.mu
    initial_condition = args.init
    print(initial_condition)
    print(mu_array)
    if len(mu_array) == 0:
        raise ValueError('The mu parameter cannot be empty')

    if len(initial_condition) != 2:
        raise ValueError('The ODE\'s initial condition must be provided. (dxdt, x) = (a,b)')

    for mu in mu_array:
        ode_solution = solve_ivp(van_del_pol_ode, [integration_start,
                            integration_end], [initial_condition[0],
                            initial_condition[1]], t_eval=time)
        plt.plot(ode_solution.y[0], ode_solution.y[1], '-')


    plt.xlim([-3,3])
    plt.xlabel('X')
    plt.ylabel('dXdt')
    plt.title('Van der Pol Oscillator Simulation ')
    plt.legend([f'$\mu = {mu}$' for mu in mu_array])
    plt.axes().set_aspect(1)
    plt.savefig(path + '/van_der_pol/van_der_pol.png')



if __name__=='__main__':
    parser = argparse.ArgumentParser(description='simulation of the van der Pol Model')
    parser.add_argument('--mu', type=list, default=[-1, 0,1,2,3,4,5,6],
        help='mu parameter, which indicates the nonlinearity and strength of damping')
        
    parser.add_argument('--init', type=list, default=[1, 0],
                    help='initial condition for the IVP')
    main(parser.parse_args())
