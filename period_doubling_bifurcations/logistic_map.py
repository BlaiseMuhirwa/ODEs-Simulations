from tracemalloc import start
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os


path = os.getcwd() + '/period_doubling_bifurcations/'
def logistic_map(r, x):
    """
        Definition of the logistic map: 
        \phi(x) = rx(1-x)
    """
    return r * x * (1 - x)

logistic_map_plotted = False
def plot_logistic_map():
    global logistic_map_plotted
    if not logistic_map_plotted:
        x = np.linspace(0, 1)
        fig, ax = plt.subplots(1, 1)
        ax.plot(x, logistic_map(2, x), 'k')
        fig.savefig(path + 'logistic_map.png')
        logistic_map_plotted = True

def plot_elements_in_orbit():
    N = 10000
    r = np.linspace(0, 4, N)
    x = np.ones(N) * 0.00001

    """ Plotting the elements in the orbit """
    fig, ax1 = plt.subplots(1, 1, figsize=(8, 9),
                               sharex=True)
    start_plot = False
    for index in range(1000):
        if index >= 900:
            start_plot = True
        x = logistic_map(r, x)
        if start_plot:
            ax1.plot(r, x, ',k', alpha=.25)
    ax1.set_xlim(1, 4)
    ax1.set_title("Bifurcations for Logistic Map")
    fig.savefig(path + 'bifurcation.png')


if __name__=='__main__':
    plot_logistic_map()
    plot_elements_in_orbit()
