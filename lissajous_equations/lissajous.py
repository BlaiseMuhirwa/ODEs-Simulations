from math import cos, sin
import matplotlib.pyplot as plt
import numpy as np
import argparse
import os
import math 

path = os.getcwd()

if __name__=='__main__':
    a = [1, 2]
    b = [1, 2, 4, 8]
    time = np.linspace(0, 100, 1000)
    for i in a:
        for j in b:
            plt.plot(np.cos(i * time), np.sin(j * time))

    plt.xlim([-1,1])
    plt.ylim([-1, 1])
    plt.xlabel('X')
    plt.ylabel('Y')
    plt.title('Lissajous Curves - (a,b) $\in \mathbb{Z}$')    
    plt.savefig(path + '/lissajous_equations/lissajous.png')
    plt.close()

    """ Try using a constant \lambda """
    lbda = 0.1
    for i in a:
        for j in b:
            plt.plot(np.cos(lbda * i * time), np.sin(lbda * j * time))

    plt.xlim([-1,1])
    plt.ylim([-1, 1])
    plt.xlabel('X')
    plt.ylabel('Y')
    plt.title('Lissajous Curves - (a,b) $\in \mathbb{Z}$, $\lambda = 0.1$')    
    plt.savefig(path + '/lissajous_equations/lissajous_with_lbda.png')

    """ Experiment with rational numbers"""
    b = [(1 + math.sqrt(5))/2, math.sqrt(2)]
    for i in a:
        for j in b:
            plt.plot(np.cos(lbda * i * time), np.sin(lbda * j * time))

    plt.xlim([-1,1])
    plt.ylim([-1, 1])
    plt.xlabel('X')
    plt.ylabel('Y')
    plt.title('Lissajous Curves -  (a$\in \mathbb{Z}, b Irrational)$')    
    plt.savefig(path + '/lissajous_equations/lissajous_b_irrational.png')


