import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde, powerlaw, pareto
from scipy.stats import ks_2samp, anderson

# Read sizes from input
def read_sizes(num_of_jobs):
    sizes = []
    for i in range(num_of_jobs):
        single_size = tuple(int(x) for x in input().split(','))[2] // 1000000
        sizes.append(single_size)
    print("Finished parsing jobs...")
    return sizes

# Fit Gaussian KDE
def fit_kde(sizes_array):
    kde = gaussian_kde(sizes_array)
    return kde

# Fit Power Law distribution
def fit_powerlaw(sizes_array):
    params_powerlaw = powerlaw.fit(sizes_array)
    return params_powerlaw

# Fit Pareto distribution
def fit_pareto(sizes_array):
    params_pareto = pareto.fit(sizes_array)
    return params_pareto

# Calculate KS statistic
def calculate_ks_statistic(data, distribution, params):
    cdf_data = np.sort(data)
    cdf_distribution = distribution.cdf(cdf_data, *params)
    ks_statistic, _ = ks_2samp(cdf_data, cdf_distribution)
    return ks_statistic

# Calculate AD statistic
def calculate_ad_statistic(data, distribution, params):
    ad_statistic = anderson(data, distribution).statistic
    return ad_statistic

# Main function
def main():
    # Number of jobs
    num_of_jobs = 5 * (10 ** 6)
    
    # Read sizes from input
    sizes = read_sizes(num_of_jobs)
    sizes_array = np.array(sizes)
    
    # Fit Gaussian KDE
    kde = fit_kde(sizes_array)
    
    # Fit Power Law distribution
    params_powerlaw = fit_powerlaw(sizes_array)
    
    # Fit Pareto distribution
    params_pareto = fit_pareto(sizes_array)
    
    print(params_pareto, params_powerlaw)
    # Calculate KS statistic for Power Law
    ks_statistic_powerlaw = calculate_ks_statistic(sizes_array, powerlaw, params_powerlaw)
    
    # Calculate AD statistic for Power Law
    ad_statistic_powerlaw = calculate_ad_statistic(sizes_array, 'expon', params_powerlaw)
    
    # Calculate KS statistic for Pareto
    ks_statistic_pareto = calculate_ks_statistic(sizes_array, pareto, params_pareto)
    
    # Calculate AD statistic for Pareto
    ad_statistic_pareto = calculate_ad_statistic(sizes_array, 'weibull_min', params_pareto)
    
    # Print statistics
    print("Power Law:")
    print("KS statistic:", ks_statistic_powerlaw)
    print("AD statistic:", ad_statistic_powerlaw)
    print()

    print("Pareto:")
    print("KS statistic:", ks_statistic_pareto)
    print("AD statistic:", ad_statistic_pareto)

# Call main function
if __name__ == "__main__":
    main()
