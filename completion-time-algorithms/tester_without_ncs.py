from rr import RR_scheduler
from sjf import SJF_scheduler
from spjf import SPJF_scheduler
from prr_optimized import PRR_scheduler
from ncs import NCS_scheduler
from oracles import GaussianPerturbationOracle, PerfectOracle, JobMeanOracle
from job_class import Job
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import random
import time

def moving_average(data, window_size):
    weights = np.repeat(1.0, window_size) / window_size
    return np.convolve(data, weights, 'valid')

# Function to run the simulation and calculate empirical competitive ratio
def run_simulation(sample_size, alpha, num_runs, num_instances, ParetoBool):
    rr_competitive_ratios = []
    SPJF_competitive_ratios = []
    prr_competitive_ratios = []
    # ncs_competitive_ratios = []
    total_spjf_time = 0
    total_prr_time = 0
    # total_ncs_time = 0
    np.random.seed(27)
     
    samples = []
    if ParetoBool:
        print("Generating pareto samples...")
        # Generate pareto samples to run in each parameter setting
        for i in range(num_instances):
            pareto_samples = np.random.pareto(alpha, sample_size)
            min_value = min(pareto_samples)
            normalized_samples = pareto_samples * (2 / min_value) 
            samples.append([[0, i, j_size] for j_size in normalized_samples]) # Instance creation
  
    else:
        print("Parsing real samples...")
        # Real samples 
        filename = r"task_lines.txt"
        with open(filename, "r") as f:
            for i in range(num_instances):
                this_instance = []
                for j in range(sample_size):
                    a = [float(x) for x in f.readline().split(",")]
                    a[0] = 0
                    a[2] /= 1000000
                    this_instance.append(a)
                samples.append(this_instance)

    # Run round robin and shortest job first
    sjf_times = []
    rr_times = []
    print("Running round robin and shortest job first...")
    sample_index = 0
    samples_size = len(samples)
    for sample in samples:
        print(f"Processing sample {sample_index} of {samples_size}")
        sample_index += 1
        rrS = RR_scheduler(0.001)
        sjfS = SJF_scheduler()
        for job in sample:
            rrS.add_job(Job(job[1], job[0], job[2]))
            sjfS.add_job(Job(job[1], job[0], job[2]))

        rrS.run()
        sjfS.run()
        sjf_times.append(sjfS.total_completion_time)
        rr_times.append(rrS.total_completion_time)

    mean_sjf = np.mean(sjf_times)
    mean_rr_cr = np.mean(rr_times) / mean_sjf

    for run_index in range(num_runs):
        this_round_SPJF_times = []
        this_round_prr_times = []
        # this_round_ncs_times = []
        for k, sample in enumerate(samples):
            print(f"Done round {run_index}.{k} of {num_runs}")

            gaussian_oracle = GaussianPerturbationOracle(0, run_index * 5000)
            job_mean_oracle = JobMeanOracle()
            # job_mean_oracle.computePredictions(sample[:max(0, 100 - run_index)])
            spjfS = SPJF_scheduler(gaussian_oracle)
            prrS = PRR_scheduler(0.5, 1, gaussian_oracle)
            # ncsS = NCS_scheduler(1, job_mean_oracle, 10)

            for index, job_size in enumerate(sample):
                spjfS.add_job(Job(job_size[1], job_size[0], job_size[2]))
                prrS.add_job(Job(job_size[1], job_size[0], job_size[2]))
                # ncsS.add_job(Job(job_size[1], job_size[0], job_size[2]))

            spjf_start = time.time()
            spjfS.run()
            spjf_end = time.time()
            prrS.run()
            prr_end = time.time()
            # ncsS.run()
            ncs_end = time.time()

            total_spjf_time += spjf_end - spjf_start
            total_prr_time  += prr_end - spjf_end
            # total_ncs_time  += ncs_end - prr_end

            this_round_SPJF_times.append(spjfS.total_completion_time)
            this_round_prr_times.append(prrS.total_completion_time)
            # this_round_ncs_times.append(ncsS.total_completion_time)

        
        rr_competitive_ratios.append(mean_rr_cr)
        SPJF_competitive_ratios.append(np.mean(this_round_SPJF_times) / mean_sjf)
        prr_competitive_ratios.append(np.mean(this_round_prr_times) / mean_sjf)
        # ncs_competitive_ratios.append(np.mean(this_round_ncs_times) / mean_sjf)

    print(f"Time spent for SPJF: {total_spjf_time}", f"Time spent for PRR: {total_prr_time}", f"Time spent for NCS: {0}", sep="\n")
    return rr_competitive_ratios, SPJF_competitive_ratios, prr_competitive_ratios#, ncs_competitive_ratios

# Parameters
alpha = 1.1
sample_size = 100
num_runs = 200
num_instances_to_average = 10
window_size = 10


# Run simulation
competitive_ratios = np.asarray(run_simulation(sample_size, alpha, num_runs, num_instances_to_average, ParetoBool=False))
competitive_ratios.dump("array_dump")
num_runs = len(competitive_ratios[0])

# Calculate moving average for each algorithm
moving_avg_data = []
for algo_data in competitive_ratios:
    moving_avg_data.append(moving_average(algo_data, window_size))

# Plot using seaborn and matplotlib
plt.figure(figsize=(10, 6))
x_axis = [i * 5000 for i in range(0, num_runs - window_size + 1)]
names = ["Round Robin", "Shortest predicted job first", "Preferential round robin ($\lambda = 0.5$)", "Non-clairvoyant scheduling ($\epsilon = 10$)"]
for i, algo_data in enumerate(moving_avg_data):
    sns.lineplot(x=x_axis, y=algo_data, label=names[i])

plt.xlabel('$\sigma$')
plt.ylabel('Average Empirical Competitive Ratio')
plt.title('Moving Average of Competitive Ratios Over Multiple Runs')
plt.xticks(rotation=45)
plt.ylim(0.5, 6)
plt.grid(True)
plt.legend()
filename = f'plots/job_plot_{random.randint(0,100)}CRAZYSIGMA.png'
print("Saved " + filename)
plt.savefig(filename)
