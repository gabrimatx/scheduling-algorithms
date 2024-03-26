import sys
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from tqdm import tqdm
from copy import deepcopy
from job_class import Job
from sjf import SJF_scheduler
from rr_optimized import RR_scheduler
from ljf import LJF_scheduler
from random_job import RAND_scheduler
from spjf import SPJF_scheduler
from spjf_dl import DSPJF_scheduler
from prr_optimized import PRR_scheduler
from oracles import (
    GaussianPerturbationOracle,
    PerfectOracle,
    JobMeanOracle,
    JobMedianOracle,
    AugmentedMedianOracle,
    AugmentedMeanOracle,
    DynamicJobMeanOracle
)
from scientific_not import sci_notation

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

    def run_simulation(self, training_slice: int, oracle_type: int) -> tuple:
        print(f"Performing test on slice {training_slice}")
        oracle_mapping = {
            0: JobMeanOracle,
            1: JobMedianOracle,
            2: AugmentedMeanOracle,
            3: AugmentedMedianOracle,
        }
        oracle_cls = oracle_mapping[oracle_type]()
        d_oracle = DynamicJobMeanOracle()
        training_set_slice = self.training_set[
            (len(self.training_set) * (10 - training_slice)) // 10 :
        ]
        if training_set_slice:
            oracle_cls.computePredictions(training_set_slice)
            d_oracle.computePredictions(training_set_slice)
        spjf_sched, prr_sched = SPJF_scheduler(oracle_cls), PRR_scheduler(
            0.5, oracle_cls
        )
        dspjf_sched = DSPJF_scheduler(d_oracle)

        spjf_sched.add_job_set(deepcopy(self.test_set))
        spjf_sched.run()

        prr_sched.add_job_set(deepcopy(self.test_set))
        prr_sched.run()

        dspjf_sched.add_job_set(deepcopy(self.test_set))
        dspjf_sched.run()

        return (
            self.rr_cr,
            spjf_sched.total_completion_time / self.sjf_tct,
            prr_sched.total_completion_time / self.sjf_tct,
            dspjf_sched.total_completion_time / self.sjf_tct,
            self.ljf_cr,
            self.rand_cr,
        )


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Please provide integer arguments.")
        sys.exit(1)
    try:
        power_for_test = int(sys.argv[1])
        oracle_type = int(sys.argv[2])
    except ValueError:
        print("Invalid integer argument provided.")
        sys.exit(1)

    tester = Tester(
        10 ** (power_for_test + 1), 5 * 10**power_for_test, "complete_jobset.csv"
    )

    slices = list(range(11))
    results = [tester.run_simulation(slice_val, oracle_type) for slice_val in slices]

    full_data = list(zip(*results))

    plt.figure(figsize=(16, 9))
    x_axis = np.arange(0, 110, 10)

    names = [
        "Round Robin",
        "Shortest predicted job first",
        "Preferential round robin",
        "Dynamic SPJF",
        "Longest Job First",
        "Random scheduling",
    ]
    oracle_mapping = {
        0: JobMeanOracle,
        1: JobMedianOracle,
        2: AugmentedMeanOracle,
        3: AugmentedMedianOracle,
    }
    for algo_data, name in zip(full_data, names):
        sns.lineplot(x=x_axis, y=algo_data, label=name)
    sns.lineplot(x=x_axis, y=1, label = "Optimal")
    plt.xlabel("Slice of the training set")
    plt.ylabel("Average Empirical Competitive Ratio")
    plt.title(
        f"Competitive ratios with job means predictions: Training_size = 10^{power_for_test + 1} | Testing_size = 5 * 10^{power_for_test}"
    )
    plt.xticks(rotation=45)
    plt.ylim(0, 10)
    plt.grid(True)
    plt.legend()
    oracle_type_name = [
        "google-mean-oracle",
        "google-median-oracle",
        "google-augmented_mean-oracle",
        "google-augmented_median-oracle",
    ]
    job_num_name = (
        f"{15 * 10 ** (power_for_test - 6)}M"
        if power_for_test >= 6
        else f"{15 * 10 ** (power_for_test - 3)}k"
    )
    filename = f"completion-time-algorithms-dl/plots/{oracle_type_name[oracle_type]}/{power_for_test}_{oracle_mapping[oracle_type].__name__}_{job_num_name}.png"
    print("Saved " + filename)
    plt.savefig(filename)
