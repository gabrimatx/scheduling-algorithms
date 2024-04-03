import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt

buckets = {}
data = pd.read_csv("complete_jobset.csv", nrows=10**7)
for idx, row in tqdm(data.iterrows(), total=len(data), desc="Processing rows..."):
    if row["job_id"] in buckets:
        buckets[row["job_id"]] = np.append(buckets[row["job_id"]], float(row["job_size"]))
    else:
        buckets[row["job_id"]] = np.array([float(row["job_size"])])


bucket_means_stds = []
for job_id, sizes in tqdm(buckets.items(), desc="Processing information..."):
    if len(sizes) > 0:
        mean = np.mean(sizes)
        std = np.std(sizes)
        bucket_means_stds.append((job_id, mean, std))

df_means_stds = pd.DataFrame(bucket_means_stds, columns=['Job_ID', 'Mean', 'Std'])

# Write DataFrame to CSV file
df_means_stds.to_csv('means_stds.csv', index=False)
