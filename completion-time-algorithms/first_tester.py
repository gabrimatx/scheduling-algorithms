from round_robin import RR_scheduler
from shortest_job_first import SJF_scheduler
from shortest_predicted_job_first import SPJF_scheduler
from preferential_round_robin import PRR_scheduler
from nonclairvoyant_scheduling_preds import NCS_scheduler
from oracles import GaussianPerturbationOracle
from job_class import Job
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

def moving_average(data, window_size):
    weights = np.repeat(1.0, window_size) / window_size
    return np.convolve(data, weights, 'valid')

# Function to run the simulation and calculate empirical competitive ratio
def run_simulation(sample_size, alpha, num_runs, num_instances):
    rr_competitive_ratios = []
    SPJF_competitive_ratios = []
    prr_competitive_ratios = []
    ncs_competitive_ratios = []

    # Generate pareto samples to run in each parameter setting
    samples = []
    np.random.seed(27)
    for i in range(num_instances):
        pareto_samples = np.random.pareto(alpha, sample_size)
        min_value = min(pareto_samples)
        normalized_samples = pareto_samples * (1 / min_value)
        samples.append(normalized_samples)

    # Run round robin and shortest job first
    sjf_times = []
    rr_times = []
    for sample in samples:
        rrS = RR_scheduler(0.001)
        sjfS = SJF_scheduler()
        for index, job_size in enumerate(sample):
            rrS.add_job(Job(index, 0, job_size))
            sjfS.add_job(Job(index, 0, job_size))

        rrS.run()
        sjfS.run()
        sjf_times.append(sjfS.total_completion_time)
        rr_times.append(rrS.total_completion_time)

    mean_sjf = np.mean(sjf_times)
    mean_rr_cr = np.mean(rr_times) / mean_sjf

    for _ in range(num_runs):
        this_round_SPJF_times = []
        this_round_prr_times = []
        this_round_ncs_times = []
        for k, sample in enumerate(samples):
            print(f"Done round {_}.{k}")

            gaussian_oracle = GaussianPerturbationOracle(0, _ * 1000)
            spjfS = SPJF_scheduler(gaussian_oracle)
            prrS = PRR_scheduler(0.5, 1, gaussian_oracle)
            ncsS = NCS_scheduler(10, gaussian_oracle, 100)

            for index, job_size in enumerate(sample):
                spjfS.add_job(Job(index, 0, job_size))
                prrS.add_job(Job(index, 0, job_size))
                ncsS.add_job(Job(index, 0, job_size))
            spjfS.run()
            prrS.run()
            ncsS.run()

            this_round_SPJF_times.append(spjfS.total_completion_time)
            this_round_prr_times.append(prrS.total_completion_time)
            this_round_ncs_times.append(ncsS.total_completion_time)

        
        rr_competitive_ratios.append(mean_rr_cr)
        SPJF_competitive_ratios.append(np.mean(this_round_SPJF_times) / mean_sjf)
        prr_competitive_ratios.append(np.mean(this_round_prr_times) / mean_sjf)
        ncs_competitive_ratios.append(np.mean(this_round_ncs_times) / mean_sjf)

    return rr_competitive_ratios, SPJF_competitive_ratios, prr_competitive_ratios, ncs_competitive_ratios

# Parameters
alpha = 1.1
sample_size = 100
num_runs = 200
num_instances_to_average = 5
window_size = 10


# Run simulation
competitive_ratios = np.asarray(run_simulation(sample_size, alpha, num_runs, num_instances_to_average))
competitive_ratios.dump("array_dump")
num_runs = len(competitive_ratios[0])

# Calculate moving average for each algorithm
moving_avg_data = []
for algo_data in competitive_ratios:
    moving_avg_data.append(moving_average(algo_data, window_size))

# Plot using seaborn and matplotlib
plt.figure(figsize=(10, 6))
x_axis = [i * 1000 for i in range(0, num_runs - window_size + 1)]
names = ["Round Robin", "Shortest predicted job first", "Preferential round robin", "Non-clairvoyant scheduling"]
for i, algo_data in enumerate(moving_avg_data):
    sns.lineplot(x=x_axis, y=algo_data, label=names[i])

plt.xlabel('Standard deviation $\sigma$')
plt.ylabel('Average Empirical Competitive Ratio')
plt.title('Moving Average of Competitive Ratios Over Multiple Runs')
plt.xticks(rotation=45)
plt.ylim(0.5, 6)
plt.grid(True)
plt.legend()
plt.savefig('200_jobs_plot.png')
plt.show()
