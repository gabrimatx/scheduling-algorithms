import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Read data from CSV file
df_means_stds = pd.read_csv("means_stds.csv")

# Calculate the maximum mean value
max_mean = df_means_stds['Mean'].max()
max_std = df_means_stds['Std'].max()
delim = max(max_mean, max_std)
# Create scatter plot using Seaborn
plt.figure(figsize=(10, 6)) 

sns.scatterplot(x='Mean', y='Std', data=df_means_stds, alpha=0.5)
plt.title('Means vs. Standard Deviations of Job Sizes')
plt.xlabel('Mean Job Size')
plt.ylabel('Standard Deviation of Job Size')
plt.grid(True)
plt.savefig("BucketScatter.pdf")
plt.show()