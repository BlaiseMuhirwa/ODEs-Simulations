
from time import time
from tracemalloc import stop
import numpy as np
import platform
import matplotlib.pyplot as plt
from scipy.integrate import solve_ivp
import argparse
import os


path = os.getcwd()

def main(args):
    sigma = args.sigma
    beta = args.beta 
    rho = args.rho
    initial, stop = 0.0, 40.0
    initial_value = args.init_val

    def lorenz_ode(time, array):
        """ define the lorenz system """
        return np.array(
                [sigma * (array[1] - array[0]),
                array[0] * (rho - array[2]) - array[1],
                array[0]*array[1] - beta * array[2]]
              )

    """ Solve the initial value problem with solve_ivp """
    solution = solve_ivp(lorenz_ode, [initial, stop], initial_value)
    time, x, y, z = solution.t, solution.y[0, :], solution.y[1, :], solution.y[2,:]

    """ plotting the trajectory of X(t) over time """
    plt.plot(time, x, linewidth=2, color='b')

    plt.grid(True)
    plt.xlabel('Time')
    plt.ylabel('X(t)')
    plt.title('X(t) Trajectory Over Time')
    plt.savefig('x_traj.png')
    plt.show(block=False)
    plt.close()

    """ plotting the solution in the 3D phase space """
    figure = plt.figure()
    axis = figure.gca(projection='3d')
    axis.plot(x,y,z, linewidth=1, color='g')
    axis.grid(True)
    axis.set_xlabel('X')
    axis.set_ylabel('Y')
    axis.set_zlabel('Z')
    axis.set_title('Lorenz ODE in 3D Phase Space')
    plt.savefig('3d_phase_space.png')
    plt.show(block=False)
    plt.close()


if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Simulation of the Lorenz System')
    parser.add_argument('--sigma', type=int, default=10, help='')
    parser.add_argument('--beta', type=float, default=8/3, help='')
    parser.add_argument('--rho', type=int, default=28, help='')
    parser.add_argument('--init_val', type=list, default=[8.0, 1.0, 1.0], 
                          help='initial value conditions for the ODE')

    main(parser.parse_args())

