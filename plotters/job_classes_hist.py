import numpy as np
import matplotlib.pyplot as plt

classes = []
num_of_jobs = 3000000
for i in range(num_of_jobs):
    single_size = tuple(int(x) for x in input().split(','))[1]
    classes.append(single_size)


classes_array = np.array(classes)
bin_edges = np.unique(classes_array)
bin_edges = np.append(bin_edges, bin_edges[-1] + 1)

hist, _ = np.histogram(classes_array, bins=bin_edges)

# Plot the histogram
plt.figure(figsize=(8, 6))
plt.bar(bin_edges[:-1], hist, width=np.diff(bin_edges), align='edge')
plt.xlabel('Classes')
plt.ylabel('Frequency')
plt.title('Histogram with One Bin Per Class')
plt.grid(True)
plt.show()
