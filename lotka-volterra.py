import numpy
import matplotlib as plt
import scipy
import pylab 
import argparse




def main(args):
    pass

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

    args = parser.parse_args()
    main(args)