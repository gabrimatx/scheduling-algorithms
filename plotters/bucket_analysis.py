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

bucket_means = {}
bucket_stds = {}
for job_id, sizes in tqdm(buckets.items(), total=len(buckets), desc="Processing information..."):
    bucket_means[job_id] = np.mean(sizes)
    bucket_stds[job_id] = np.std(sizes)

df_means_stds = pd.DataFrame({'Job_ID': list(bucket_means.keys()), 'Mean': list(bucket_means.values()), 'Std': list(bucket_stds.values())})

# Write DataFrame to CSV file
df_means_stds.to_csv('means_stds.csv', index=False)

# Create DataFrame for visualization
df_means_stds = pd.DataFrame({'Mean': bucket_means, 'Std': bucket_stds})

# Plot means vs. standard deviations
plt.figure(figsize=(10, 6))
plt.scatter(df_means_stds['Mean'], df_means_stds['Std'], alpha=0.5)
plt.title('Means vs. Standard Deviations of Job Sizes')
plt.xlabel('Mean Job Size')
plt.ylabel('Standard Deviation of Job Size')
plt.grid(True)
plt.savefig("BucketScatter.pdf")
plt.show()