import sys
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from tqdm import tqdm
from copy import deepcopy
from job_class import Job
from scheduler_sjf import SJF_scheduler, DSPJF_scheduler, SPJF_scheduler
from ncs import NCS_scheduler
from scheduler_rr import *
from scheduler_others import *
from scheduler_prr import *
from oracles import (
    GaussianPerturbationOracle,
    PerfectOracle,
    JobMeanOracle,
    JobMedianOracle,
    AugmentedMedianOracle,
    AugmentedMeanOracle,
    DynamicJobMeanOracle,
)

np.random.seed(27)


class Tester:
    def __init__(
        self, sample_training_size: int, sample_test_size: int, input_file: str
    ):
        self.training_set = []
        self.test_set = []
        data = pd.read_csv(input_file, nrows=sample_training_size + sample_test_size)
        for idx, row in tqdm(
            data.iterrows(), total=len(data), desc="Parsing jobs from dataset"
        ):
            job = Job(
                row["job_id"], row["arrival_time"] / 10**6, row["job_size"] / 10**6
            )
            if idx < sample_training_size:
                self.training_set.append(job)
            else:
                self.test_set.append(job)
        self.sjf_tct = self.compute_scheduler_tct(SJF_scheduler)
        self.rr_cr = self.compute_scheduler_cr(RR_scheduler)
        self.ljf_cr = self.compute_scheduler_cr(LJF_scheduler)
        self.rand_cr = self.compute_rand_cr()

    def compute_scheduler_tct(self, scheduler_cls) -> int:
        print(f"Running {scheduler_cls.__name__} on test set...")
        scheduler = scheduler_cls()
        scheduler.add_job_set(deepcopy(self.test_set))
        scheduler.run()
        return scheduler.total_completion_time

    def compute_scheduler_cr(self, scheduler_cls) -> int:
        print(f"Running {scheduler_cls.__name__} on test set...")
        scheduler = scheduler_cls()
        scheduler.add_job_set(deepcopy(self.test_set))
        scheduler.run()
        return scheduler.total_completion_time / self.sjf_tct

    def compute_rand_cr(self) -> int:
        print("Running random processing on test set...")
        big_completion_time = 0
        for i in range(10):
            print(f"Running random instance {i} of 10")
            rand_scheduler = RAND_scheduler()
            rand_scheduler.add_job_set(deepcopy(self.test_set))
            rand_scheduler.run()
            big_completion_time += rand_scheduler.total_completion_time / self.sjf_tct
            rand_scheduler.total_completion_time = 0
            rand_scheduler.queue = []
        return big_completion_time / 10

    def run_simulation(self, training_slice: int) -> tuple:
        print(f"Performing test on slice {training_slice}...")

        prediction_schedulers = [
            SPJF_scheduler(None),
            PRR_scheduler(0.5, None),
            PRR_scheduler(0.25, None),
            PRR_scheduler(0.75, None),
        ]

        dynamic_prediction_schedulers = [
            DSPJF_scheduler(None),
            DPRR_scheduler(0.5, None),
        ]

        s_oracle = JobMeanOracle()
        dynamic_oracles = [DynamicJobMeanOracle() for ds in dynamic_prediction_schedulers]

        training_set_slice = self.training_set[
            (len(self.training_set) * (10 - training_slice)) // 10 :
        ]

        if training_set_slice:
            s_oracle.computePredictions(training_set_slice)
            for dynamic_oracle in dynamic_oracles:
                dynamic_oracle.computePredictions(training_set_slice)

        for ind, scheduler in enumerate(prediction_schedulers):
            scheduler.oracle = s_oracle
            scheduler.add_job_set(deepcopy(self.test_set))
            scheduler.run()

        for ind, scheduler in enumerate(dynamic_prediction_schedulers):
            scheduler.oracle = dynamic_oracles[ind]
            scheduler.add_job_set(deepcopy(self.test_set))
            scheduler.run()

        return (
            [self.rr_cr]
            + [
                scheduler.total_completion_time / self.sjf_tct
                for scheduler in prediction_schedulers
            ]
            + [
                scheduler.total_completion_time / self.sjf_tct
                for scheduler in dynamic_prediction_schedulers
            ]
            + [self.ljf_cr, self.rand_cr]
        )


if __name__ == "__main__":
    training_size = 10**5 * 2
    test_size = 10**5

    tester = Tester(training_size, test_size, "complete_jobset.csv")

    slices = list(range(11))
    results = [tester.run_simulation(slice_val) for slice_val in slices]

    full_data = list(zip(*results))

    plt.figure(figsize=(16, 9))
    x_axis = np.arange(0, 110, 10)

    names = [
        "Round Robin",
        "Shortest predicted job first",
        r"Preferential round robin $\lambda = 0.5$",
        r"Preferential round robin $\lambda = 0.25$",
        r"Preferential round robin $\lambda = 0.75$",
        "Dynamic SPJF",
        "Dynamic PRR",
        "Longest Job First",
        "Random scheduling",
    ]

    for algo_data, name in zip(full_data, names):
        sns.lineplot(x=x_axis, y=algo_data, label=name)
    sns.lineplot(x=x_axis, y=1, label="Optimal")
    plt.xlabel("Slice of the training set")
    plt.ylabel("Average Empirical Competitive Ratio")
    plt.title(f"Competitive ratios with job means predictions")
    plt.xticks(rotation=45)
    plt.ylim(0, 4.5)
    plt.grid(True)
    plt.legend()

    filename = f"new_name.png"
    print("Saved " + filename)
    plt.savefig(filename)
