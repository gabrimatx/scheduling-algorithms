import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde

sizes = []
num_of_jobs = 3 * (10 ** 6)
for i in range(num_of_jobs):
	single_size = tuple(int(x) for x in input().split(','))[2] // 10000
	sizes.append(single_size)

sizes_array = np.array(sizes)
kde = gaussian_kde(sizes_array)

x_vals = np.linspace(0, np.max(sizes_array), 100)
pdf_estimation = kde(x_vals)

plt.figure(figsize=(8, 6))
plt.plot(x_vals, pdf_estimation, label='PDF Approximation', color='blue')
plt.fill_between(x_vals, pdf_estimation, color='skyblue', alpha=0.3)
plt.xlabel('Sizes')
plt.ylabel('Probability Density')
plt.title('Probability Density Plot Approximation')
plt.legend()
plt.grid(True)
plt.show()