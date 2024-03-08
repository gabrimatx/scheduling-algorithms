from round_robin import RR_scheduler
from shortest_job_first import SJF_scheduler
from shortest_predicted_job_first import SPJF_scheduler
from preferential_round_robin import PRR_scheduler
from oracles import GaussianPerturbationOracle
from job_class import Job
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# Function to run the simulation and calculate empirical competitive ratio
def run_simulation(sample_size, alpha, num_runs):
    rr_competitive_ratios = []
    SPJF_competitive_ratios = []
    prr_competitive_ratios = []


    for _ in range(num_runs):
        print(f"Done round {_}")
        round_robin_quantum = 0.000001
        rrS = RR_scheduler(round_robin_quantum)
        sjfS = SJF_scheduler()
        gaussian_oracle = GaussianPerturbationOracle(0, _ * 50)
        spjfS = SPJF_scheduler(gaussian_oracle)
        prrS = PRR_scheduler(0.5, 1, gaussian_oracle)

        pareto_samples = np.random.pareto(alpha, sample_size)

        # Find the minimum value in the Pareto samples
        min_value = min(pareto_samples)

        # Normalize the Pareto samples
        normalized_samples = pareto_samples * (1 / min_value)

        for index, job_size in enumerate(normalized_samples):
            rrS.add_job(Job(index, 0, job_size))
            sjfS.add_job(Job(index, 0, job_size))
            spjfS.add_job(Job(index, 0, job_size))
            prrS.add_job(Job(index, 0, job_size))

        sjfS.run()
        spjfS.run()
        rrS.run()
        prrS.run()

        rr_completion_time = rrS.total_completion_time
        sjf_completion_time = sjfS.total_completion_time
        spjfS_completion_time = spjfS.total_completion_time
        prr_completion_time = prrS.total_completion_time

        rr_competitive_ratios.append(rr_completion_time / sjf_completion_time)
        SPJF_competitive_ratios.append(spjfS_completion_time / sjf_completion_time)
        prr_competitive_ratios.append(prr_completion_time /sjf_completion_time)

    return rr_competitive_ratios, SPJF_competitive_ratios, prr_competitive_ratios

# Parameters
alpha = 1.1
sample_size = 50
num_runs = 20

# Run simulation multiple times
competitive_ratios = run_simulation(sample_size, alpha, num_runs)

plt.figure(figsize=(10, 6))
plt.plot(range(1, num_runs + 1), competitive_ratios[0], marker='o', color='skyblue', label='Round Robin')
plt.plot(range(1, num_runs + 1), competitive_ratios[1], marker='o', color='red', label='SPJF')
plt.plot(range(1, num_runs + 1), competitive_ratios[2], marker='o', color='green', label='PRR')

plt.xlabel('Run')
plt.ylabel('Average Empirical Competitive Ratio')
plt.title('Competitive Ratio Over Multiple Runs, prediction error increases 50 times more each time')
plt.xticks(rotation=45)
plt.grid(True)

# Add legend
plt.legend()

plt.show()
