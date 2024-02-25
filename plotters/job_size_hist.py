import numpy as np
import matplotlib.pyplot as plt

sizes = []
num_of_jobs = 3 * (10 ** 6)
for i in range(num_of_jobs):
	single_size = tuple(int(x) for x in input().split(','))[2] // 10000
	sizes.append(single_size)


sizes_array = np.array(sizes)

num_intervals = 100

bin_edges = np.linspace(sizes_array.min(), sizes_array.max(), num_intervals + 1) // 20

hist, _ = np.histogram(sizes_array, bins=bin_edges)

# Plot the histogram
plt.figure(figsize=(8, 6))
plt.bar(bin_edges[:-1], hist, width=np.diff(bin_edges), align='edge')
plt.xlabel('Sizes')
plt.ylabel('Frequency')
plt.title('Histogram with Discretized Intervals')
plt.grid(True)
plt.show()
