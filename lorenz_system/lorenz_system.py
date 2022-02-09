
from tracemalloc import stop
import numpy as np
import platform
import matplotlib.pyplot as plt
from scipy.integrate import solve_ivp
import argparse


def main(args):
    sigma = args.sigma
    beta = args.beta 
    rho = args.rho
    initial, stop = 0.0, 40.0
    initial_value = args.initial_value

    def lorenz_ode(array):
        """ define the lorenz system """
        return np.array(
                [sigma * (array[1] - array[0]),
                array[0] * (rho - array[2]) - array[1],
                array[0]*array[1] - beta * array[2]]
              )

    """ Solve the initial value problem with solve_ivp """
    solution = solve_ivp


    
    
          





def lorenz_deriv(t, xyz):
    beta, rho, sigma, t0, y0, tstop = lorenz_parameters()
    dxdt = np.zeros(3)

    dxdt[0] = sigma * (xyz[1] - xyz[0])
    dxdt[1] = xyz[0] * (rho - xyz[2]) - xyz[1]
    dxdt[2] = xyz[0] * xyz[1] - beta * xyz[2]

    return dxdt


def lorenz_ode_test():
    beta, rho, sigma, t0, xyz0, tstop = lorenz_parameters()
    t, x, y, z = lorenz_ode_solve_ivp()
    lorenz_ode_plot_components(t, x, y, z)
    lorenz_ode_plot_3d(t, x, y, z)

    return


def lorenz_ode_solve_ivp():
    beta, rho, sigma, t0, xyz0, tstop = lorenz_parameters()

    tspan = np.array([t0, tstop])
    sol = solve_ivp(lorenz_deriv, tspan, xyz0)

    t = sol.t
    x = sol.y[0, :]
    y = sol.y[1, :]
    z = sol.y[2, :]

    return t, x, y, z


def lorenz_ode_plot_components(t, x, y, z):
    plt.plot(t, x, linewidth=2, color='b')
    plt.plot(t, y, linewidth=2, color='r')
    plt.plot(t, z, linewidth=2, color='g')
    plt.grid(True)
    plt.xlabel('<--- Time --->')
    plt.ylabel('<--- X(T), Y(T), Z(T) --->')
    plt.title('lorenz_ode(): Time Series Plot')
    plt.savefig('lorenz_ode_components.png')
    print('')
    print('  Graphics data saved as "lorenz_ode_components.png"')
    plt.show(block=False)
    plt.close()

    return


def lorenz_ode_plot_3d(t, x, y, z):

    fig = plt.figure()
    ax = fig.gca(projection='3d')
    ax.plot(x, y, z, linewidth=1, color='b')
    ax.grid(True)
    ax.set_xlabel('<--- X(T) --->')
    ax.set_ylabel('<--- Y(T) --->')
    ax.set_zlabel('<--- Z(T) --->')
    ax.set_title('lorenz_ode(): 3D Plot')
    filename = 'lorenz_ode_3d.png'
    plt.savefig(filename)
    print('')
    print('  Graphics saved as "' + filename + '"')
    plt.show(block=False)
    plt.close()

    return


def lorenz_parameters(beta_user=None, rho_user=None,
                      sigma_user=None, t0_user=None, y0_user=None, tstop_user=None):

    # *****************************************************************************80
    #
    # lorenz_parameters() returns parameters for lorenz_ode().
    #
    #  Discussion:
    #
    #    If input values are specified, this resets the default parameters.
    #    Otherwise, the output will be the current defaults.
    #
    #  Licensing:
    #
    #    This code is distributed under the GNU LGPL license.
    #
    #  Modified:
    #
    #    02 February 2022
    #
    #  Author:
    #
    #    John Burkardt
    #
    #  Input:
    #
    #    real BETA_USER: problem parameter.
    #
    #    real RHO_USER: problem parameter.
    #
    #    real SIGMA_USER: problem parameter.
    #
    #    real T0_USER: the initial time.
    #
    #    real Y0_USER: the initial condition.
    #
    #    real TSTOP_USER: the final time.
    #
    #  Output:
    #
    #    real BETA: problem parameter.
    #
    #    real RHO: problem parameter.
    #
    #    real SIGMA: problem parameter.
    #
    #    real T0: the initial time.
    #
    #    real Y0: the initial condition.
    #
    #    real TSTOP: the final time.
    #
    import numpy as np
#
#  Initialize defaults.
#
    if not hasattr(lorenz_parameters, "beta_default"):
        lorenz_parameters.beta_default = 8.0 / 3.0

    if not hasattr(lorenz_parameters, "rho_default"):
        lorenz_parameters.rho_default = 28.0

    if not hasattr(lorenz_parameters, "sigma_default"):
        lorenz_parameters.sigma_default = 10.0

    if not hasattr(lorenz_parameters, "t0_default"):
        lorenz_parameters.t0_default = 0.0

    if not hasattr(lorenz_parameters, "y0_default"):
        lorenz_parameters.y0_default = np.array([8.0, 1.0, 1.0])

    if not hasattr(lorenz_parameters, "tstop_default"):
        lorenz_parameters.tstop_default = 40.0
#
#  Update defaults if input was supplied.
#
    if (beta_user is not None):
        lorenz_parameters.beta_default = beta_user

    if (rho_user is not None):
        lorenz_parameters.rho_default = rho_user

    if (sigma_user is not None):
        lorenz_parameters.sigma_default = sigma_user

    if (t0_user is not None):
        lorenz_parameters.t0_default = t0_user

    if (y0_user is not None):
        lorenz_parameters.y0_default = y0_user

    if (tstop_user is not None):
        lorenz_parameters.tstop_default = tstop_user
#
#  Return values.
#
    beta = lorenz_parameters.beta_default
    rho = lorenz_parameters.rho_default
    sigma = lorenz_parameters.sigma_default
    t0 = lorenz_parameters.t0_default
    y0 = lorenz_parameters.y0_default
    tstop = lorenz_parameters.tstop_default

    return beta, rho, sigma, t0, y0, tstop





if __name__=='__main__':
  parser = argparse.ArgumentParser(description='Simulation of the Lorenz System')
  parser.add_argument('--sigma', type=int, default=10, help='')
  parser.add_argument('--beta', type=float, default=8/3, help='')
  parser.add_argument('--rho', type=int, default=28, help='')
  parser.add_argument('--init_val', type=list, default=[8.0, 1.0, 1.0], 
                          help='initial value conditions for the ODE')


  main(parser.parse_args())

  lorenz_ode_test()

