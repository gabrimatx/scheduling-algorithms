import numpy as np

# Define the shape parameter (alpha) of the Pareto distribution
alpha = 1.1

# Define the size of the sample you want to generate
sample_size = 3 * (10 ** 6)

# Generate samples from the Pareto distribution
pareto_samples = np.random.pareto(alpha, sample_size)

# Print the first 10 samples
print(np.mean(pareto_samples))
