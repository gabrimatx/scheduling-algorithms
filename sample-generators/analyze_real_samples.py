import numpy as np
import matplotlib.pyplot as plt

sizes = []
num_of_jobs = 3 * (10 ** 6)
for i in range(num_of_jobs):
	single_size = tuple(int(x) for x in input().split(','))[2] 
	sizes.append(single_size)


sizes_array = np.array(sizes)
print(max(sizes_array), min(sizes_array))
