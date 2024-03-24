import numpy as np

alpha = 1.1

sample_size = 3 * (10 ** 6)

pareto_samples = np.random.pareto(alpha, sample_size)

print(np.mean(pareto_samples))
