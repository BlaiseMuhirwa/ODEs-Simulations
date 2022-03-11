import math
from re import I
from turtle import color 
import numpy as np
import matplotlib.pyplot as plt
import os 
import copy


PATH = os.getcwd()

if __name__=='__main__':
    N = 1000
    frequencies = {
        1: 0, 2: 0, 3: 0, 4: 0,
        5: 0, 6: 0, 7: 0, 8: 0, 9:0
    }
    new_frequencies = copy.deepcopy(frequencies)
    for i in range(N):
        current_num = 2 ** i
        while current_num >= 10:
            current_num //= 10
        frequencies[current_num] += 1

    plt.bar(frequencies.keys(), frequencies.values(), width=0.8, color='grey')
    plt.title('Histogram of First Digits - $2^n$')
    plt.savefig(PATH + '/benford_law/benford_2.png')
    plt.close()

    """ repeating the same experiment with 3^n"""
    for j in range(N):
        current_num = 3 ** j
        while current_num >= 10:
            current_num //= 10
        new_frequencies[current_num] += 1
    
    plt.bar(new_frequencies.keys(), new_frequencies.values(), width=0.8, color='grey')
    plt.title('Histogram of First Digits - $3^n$')
    plt.savefig(PATH + '/benford_law/benford_3.png')

    """ repeating the same experiment with 13^n"""
    last_frequencies = copy.deepcopy(frequencies)
    for j in range(N):
        current_num = 13 ** j
        while current_num >= 10:
            current_num //= 10
        last_frequencies[current_num] += 1
    
    plt.bar(last_frequencies.keys(), last_frequencies.values(), width=0.8, color='grey')
    plt.title('Histogram of First Digits - $13^n$')
    plt.savefig(PATH + '/benford_law/benford_13.png')
        
        



        
        