

from tokenize import Double
import numpy as np
import pandas as pd
import os
import copy

class IntegrationMethods:
    def __init__(self, step_size: None, initial_condition, range) -> None:
        self.step_size = step_size
        self.initial_condition = initial_condition
        self.previous_value = self.initial_condition
        self.range = range

    def euler_method(self) -> Double:
        next_val = self.previous_value + (self.step_size * self.previous_value)
        self.previous_value = next_val
        return next_val

    def midpoint_method(self) -> Double:
        intermediate_point = self.previous_value + (self.step_size / 2) * self.previous_value
        next_val = self.previous_value + (self.step_size * intermediate_point)
        self.previous_value = next_val
        return next_val

    def runge_kutta_method(self) -> Double:
        k1 = self.previous_value
        k2 = self.previous_value + (self.step_size * (k1/2))
        k3 = self.previous_value + (self.step_size * (k2/2))
        k4 = self.previous_value + (self.step_size * k3)
        sum_of_terms = (k1 + 2 * k2 + 2 * k3 + k4)
        next_val = self.previous_value + ((1/6) * self.step_size * sum_of_terms)
        self.previous_value = next_val

        return next_val

    def compute_results(self):
        """ computing results for Euler's method """
        euler_approximation = {"N": [], "relative_error": [], "absolute_error": []}
        midpoint_approximation = copy.deepcopy(euler_approximation)
        runge_kutta_approximation = copy.deepcopy(euler_approximation)

        prev_values = [self.initial_condition, self.initial_condition,
                        self.initial_condition]
        

        for index in self.range:
            N = 2 ** index
            euler_approximation['N'].append(N)
            midpoint_approximation['N'].append(N)
            runge_kutta_approximation['N'].append(N)
            step_size = 1 / N

            self.step_size = step_size
            new_value_euler = self.euler_method()
            self.previous_value = self.initial_condition
            new_value_midpoint= self.midpoint_method()

            self.previous_value = self.initial_condition
            new_value_runge_kutta = self.runge_kutta_method()
            self.previous_value = self.initial_condition

            euler_approximation['relative_error'].append(new_value_euler - prev_values[0])
            midpoint_approximation['relative_error'].append(new_value_midpoint - prev_values[1])
            runge_kutta_approximation['relative_error'].append(new_value_runge_kutta - prev_values[2])
            euler_approximation['absolute_error'].append(np.exp(1) - prev_values[0])
            midpoint_approximation['absolute_error'].append(np.exp(1) - prev_values[1])
            runge_kutta_approximation['absolute_error'].append(np.exp(1) - prev_values[2])

            prev_values[0] = new_value_euler
            prev_values[1] = new_value_midpoint
            prev_values[2] = new_value_runge_kutta

        self.step_size = None
        self.previous_value = self.initial_condition
        prev_value = self.initial_condition

        euler_results = pd.DataFrame(euler_approximation)
        midpoint_results = pd.DataFrame(midpoint_approximation)
        rk_results = pd.DataFrame(runge_kutta_approximation)
        euler_results.to_csv(os.getcwd() + '/methods_of_integration/euler_results.csv')
        midpoint_results.to_csv(os.getcwd() + '/methods_of_integration/midpoint_results.csv')
        rk_results.to_csv(os.getcwd() + '/methods_of_integration/runge_kutta.csv')



if __name__=='__main__':
    k = range(10, 21)
    obj = IntegrationMethods(step_size=None, initial_condition=1, range=k)
    obj.compute_results()
