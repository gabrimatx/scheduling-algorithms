import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from job_class import Job
from tqdm import tqdm
from copy import deepcopy
from scheduler_sjf import SJF_scheduler
from collections import defaultdict

class DataLoader:
    def __init__(self, filename: str, training_size: int, test_size: int, normalizer: int) -> None:
        self.filename = filename
        self.training_size = training_size
        self.test_size = test_size
        self.training_set = []
        self.test_set = []
        self.load_data(normalizer)
    
    def load_data(self, norm):
        data = pd.read_csv(self.filename, nrows=self.training_size + self.test_size)
        for idx, row in tqdm(data.iterrows(), total=len(data), desc="Parsing jobs from dataset"):
            job = Job(row["job_id"], row["arrival_time"] / (10 ** norm), row["job_size"]  / (10 ** norm))
            if idx < self.training_size:
                self.training_set.append(job)
            else:
                self.test_set.append(job)

    def get_training_set(self):
        return deepcopy(self.training_set)

    def get_test_set(self):
        return deepcopy(self.test_set)

class Experiment:
    def __init__(self, static_schedulers, dynamic_schedulers, static_oracle, dynamic_oracle) -> None:
        self.static_schedulers = static_schedulers
        self.dynamic_schedulers = dynamic_schedulers
        self.static_oracle = static_oracle
        self.dynamic_oracle = dynamic_oracle
    
    def setup_schedulers(self, training_set, slice, test_set):
        # Obtain slice from the training set
        training_slice = training_set[(len(training_set) * (10 - slice)) // 10 :]

        # Add static oracle to static schedulers
        s_oracle = self.static_oracle()
        if training_slice:
            s_oracle.computePredictions(training_slice)
        
        for static_scheduler in self.static_schedulers:
            static_scheduler.queue = []
            static_scheduler.total_completion_time = 0
            static_scheduler.current_time = 0
            static_scheduler.oracle = s_oracle
            static_scheduler.add_job_set(deepcopy(test_set))
        
        # Add dynamic oracles:
        for dynamic_scheduler in self.dynamic_schedulers:
            dynamic_scheduler.queue = []
            dynamic_scheduler.total_completion_time = 0
            dynamic_scheduler.current_time = 0
            dyn_oracle = self.dynamic_oracle()
            if training_slice:
                dyn_oracle.computePredictions(training_slice)
            dynamic_scheduler.oracle = dyn_oracle
            dynamic_scheduler.add_job_set(deepcopy(test_set))
    
    def run_experiment(self, result_dict, sjf):
        for static_scheduler in self.static_schedulers:
            static_scheduler.run()
            keyname = static_scheduler.name
            result_dict[keyname].append(static_scheduler.total_completion_time / sjf)

        for dynamic_scheduler in self.dynamic_schedulers:
            dynamic_scheduler.run()
            keyname = dynamic_scheduler.name
            result_dict[keyname].append(dynamic_scheduler.total_completion_time / sjf)

        

class Tester:
    def __init__(self, data_loader: DataLoader, experiment: Experiment, slices: list) -> None:
        self.data_loader = data_loader
        self.experiment = experiment
        self.slices = slices
        self.sjf_tct = self.compute_sjf_tct()
        self.results = defaultdict(list)

    def compute_sjf_tct(self):
        scheduler = SJF_scheduler()
        print(f"Running shortest job first on test set...")
        scheduler.add_job_set(self.data_loader.get_test_set())
        scheduler.run()
        return scheduler.total_completion_time

    def run_test(self):
        for slice in self.slices:
            print("Running test #", slice)
            self.experiment.setup_schedulers(self.data_loader.training_set, slice, self.data_loader.test_set)
            self.experiment.run_experiment(self.results, self.sjf_tct)

    def make_plot(self, filename = "plot.pdf", dataset_name = ""):
        df = pd.DataFrame(self.results)
        plt.figure(figsize=(15, 9))
        x_ind = 1
        for col in df.columns:
            plt.plot(df.index, df[col], marker='', label=col)
            # Annotate the lines
            plt.text(df.index[x_ind] + 0.2, df[col][x_ind] + 0.02, col, fontsize=10, ha='left', va='center')
            x_ind = (x_ind + 1) % 10
        plt.xticks(range(11), range(11))  
        plt.xlabel('Training set slices')
        plt.ylabel('Competitive ratio')
        plt.title(f'Scheduling on {dataset_name}, sizes: Test = {self.data_loader.test_size} | Train = {self.data_loader.training_size}')
        plt.grid(visible=True)
        plt.savefig(filename)



