from rr_optimized import RR_scheduler
from sjf import SJF_scheduler
from spjf import SPJF_scheduler
from prr_optimized import PRR_scheduler
from ncs import NCS_scheduler
from ljf import LJF_scheduler
from random_job import RAND_scheduler
from oracles import GaussianPerturbationOracle, PerfectOracle, JobMeanOracle, JobMedianOracle, AugmentedMedianOracle, AugmentedMeanOracle
from job_class import Job
from scientific_not import sci_notation
from tqdm import tqdm
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import random
import pandas as pd
import time
import sys
from copy import copy, deepcopy

np.random.seed(27)
class Tester:
    def __init__(self, sample_training_size: int, sample_test_size: int, input_file: str):
        self.training_set = []
        self.test_set = []
        data = pd.read_csv(input_file, nrows=sample_training_size + sample_test_size)
        for idx, row in tqdm(data.iterrows(), total = len(data), desc = "Parsing jobs from dataset"):
            job = Job(row['job_id'], row['arrival_time'] / 10 ** 6, row['job_size'] / 10 ** 6)
            if idx < sample_training_size:
                self.training_set.append(job)
            else:
                self.test_set.append(job)
        self.sjf_tct = self.compute_sjf_tct()
        self.rr_cr = self.compute_rr_cr()
        self.ljf_cr = self.compute_ljf_cr()
        self.rand_cr = self.compute_rand_cr()
    
    def compute_sjf_tct(self) -> int:
        print("Running shortest job first on test set...")
        sjf_scheduler = SJF_scheduler()
        sjf_scheduler.add_job_set(deepcopy(self.test_set))
        sjf_scheduler.run()
        return sjf_scheduler.total_completion_time
    
    def compute_rr_cr(self) -> int:
        print("Running round robin on test set...")
        rr_scheduler = RR_scheduler()
        rr_scheduler.add_job_set(deepcopy(self.test_set))
        rr_scheduler.run()
        return rr_scheduler.total_completion_time / self.sjf_tct
    
    def compute_ljf_cr(self) -> int:
        print("Running longest job first on test set...")
        ljf_scheduler = LJF_scheduler()
        ljf_scheduler.add_job_set(deepcopy(self.test_set))
        ljf_scheduler.run()
        return ljf_scheduler.total_completion_time / self.sjf_tct
    
    def compute_rand_cr(self) -> int:
        print("Running random processing on test set...")
        big_completion_time = 0
        for i in range(10):
            print("Running random instance", i, "of 10")
            rand_scheduler = RAND_scheduler()
            rand_scheduler.add_job_set(deepcopy(self.test_set))
            rand_scheduler.run()
            big_completion_time += rand_scheduler.total_completion_time / self.sjf_tct
            rand_scheduler.total_completion_time = 0
            rand_scheduler.queue = []
        return big_completion_time / 10

    def moving_average(data, window_size):
        weights = np.repeat(1.0, window_size) / window_size
        return np.convolve(data, weights, 'valid')


    def run_simulation(self, training_slice: int) -> tuple:
        print("Performing test on slice", training_slice)
        oracle = AugmentedMedianOracle()
        training_set_slice = self.training_set[(len(self.training_set) * (10 - training_slice))//10:]
        if training_set_slice:
            oracle.computePredictions(training_set_slice)
        spjf_sched, prr_sched = SPJF_scheduler(oracle), PRR_scheduler(0.5, oracle)
        spjf_sched.add_job_set(deepcopy(self.test_set))
        prr_sched.add_job_set(deepcopy(self.test_set))

        spjf_sched.run()
        prr_sched.run()

        return self.rr_cr, spjf_sched.total_completion_time / self.sjf_tct, prr_sched.total_completion_time / self.sjf_tct, self.ljf_cr, self.rand_cr



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide an integer argument.")
        raise Exception
    try:
        power_for_test = int(sys.argv[1])
    except ValueError:
        print("Invalid integer argument provided.")
        raise Exception
    tester = Tester(10 ** (power_for_test + 1), 5 * 10 ** power_for_test, "complete_jobset.csv")
    slices = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    rr_crs = []
    spjf_crs = []
    prr_crs = []
    ljf_crs = []
    rand_crs = []
    for slice in slices:
        rr_cr, spjf_cr, prr_cr, ljf_cr, rand_cr = tester.run_simulation(slice)
        rr_crs.append(rr_cr)
        spjf_crs.append(spjf_cr)
        prr_crs.append(prr_cr)
        ljf_crs.append(ljf_cr)
        rand_crs.append(rand_cr)
        np.asarray([rr_crs, spjf_crs, prr_crs, ljf_crs, rand_crs]).dump(f"completion-time-algorithms/dumps/cr_dump_{slice}")

    plt.figure(figsize=(10, 6))
    full_data = [rr_crs, spjf_crs, prr_crs, ljf_crs, rand_crs]
    x_axis = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    names = ["Round Robin", "Shortest predicted job first", "Preferential round robin", "Longest Job First", "Random scheduling"]
    for i, algo_data in enumerate(full_data):
        sns.lineplot(x=x_axis, y=algo_data, label=names[i])

    plt.xlabel('Slice of the training set')
    plt.ylabel('Average Empirical Competitive Ratio')
    plt.title(f'Competitive ratios with job means predictions: Test: 10^{power_for_test + 1} | Training: 5 * 10^{power_for_test}')
    plt.xticks(rotation=45)
    plt.ylim(0, 10)
    plt.grid(True)
    plt.legend()
    job_num_name = str(15 * 10 ** (power_for_test - 6)) + 'M' if power_for_test >= 6 else str(15 * 10 ** (power_for_test - 3)) + 'k'
    filename = f'completion-time-algorithms/plots/google-augmented_median-oracle/{job_num_name}_adj_job_plot_{job_num_name}.png'
    print("Saved " + filename)
    plt.savefig(filename)
    # plt.show()