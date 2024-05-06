import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from job_class import Job
from tqdm import tqdm
from copy import deepcopy
from scheduler_sjf import SJF_scheduler
from collections import defaultdict
from oracles import LotteryOracle
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
            if not static_scheduler.oracle:
                static_scheduler.oracle = s_oracle
            else:
                static_scheduler.oracle.computePredictions(training_slice)
            static_scheduler.add_job_set(deepcopy(test_set))
        
        # Add dynamic oracles:
        for dynamic_scheduler in self.dynamic_schedulers:
            dynamic_scheduler.queue = []
            dynamic_scheduler.total_completion_time = 0
            dynamic_scheduler.current_time = 0
            if not dynamic_scheduler.oracle:
                dynamic_scheduler.oracle = self.dynamic_oracle()
            if training_slice:
                dynamic_scheduler.oracle.computePredictions(training_slice)
            dynamic_scheduler.add_job_set(deepcopy(test_set))
    
    def run_experiment(self, result_dict, sjf):
        for scheduler in self.static_schedulers + self.dynamic_schedulers:
            scheduler.run()
            keyname = scheduler.name
            result_dict[keyname].append([scheduler.total_completion_time / sjf, np.std([res/sjf for res in scheduler.std])])

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
            print("Running test #", slice, "=" * 150)
            self.experiment.setup_schedulers(self.data_loader.training_set, slice, self.data_loader.test_set)
            self.experiment.run_experiment(self.results, self.sjf_tct)

    def make_plot(self, filename = "plot.pdf", dataset_name = ""):
        def pick_style(sched_name):
            name = sched_name.split()[0]
            switcher = {
                "RR": ('-', "blue"),
                "SPJF": ('--', "red"),
                "PRR": ('--', "green"),
                "dSPJF": ('-.', "tomato"),
                "dPRR": ('-.', "limegreen"),
                "dLambda": ('-.', "teal"),
                "dNCS": ('-.', "purple"),
                "Lottery": ('-.', "gold"),
                "NCS": ('--', "dimgray"),
            }
            return switcher.get(name, ("-", "black"))

        # Extract mean values and standard deviations for each scheduler
        mean_data = {key: [x[0] for x in values] for key, values in self.results.items()}
        std_data = {key: [x[1] for x in values] for key, values in self.results.items()}

        sns.set_theme(context="paper")
        sns.set_style("whitegrid")
        sns.set_palette("husl")

        plt.figure(figsize=(15, 9))

        # Plot the lines using Seaborn's lineplot with error bars for standard deviation
        for scheduler, mean_values in mean_data.items():
            sns.lineplot(x=range(len(mean_values)), y=mean_values, label=scheduler, color = pick_style(scheduler)[1], linestyle = pick_style(scheduler)[0])

            # Add shaded area representing standard deviation
            plt.fill_between(range(len(mean_values)), [mean_values[i] - std_data[scheduler][i] for i in range(len(mean_values))],
                            [mean_values[i] + std_data[scheduler][i] for i in range(len(mean_values))], alpha=0.1, color = pick_style(scheduler)[1])

        plt.legend(loc = "upper center", bbox_to_anchor=(0.5, 1.1), shadow=True, ncol=len(self.results))

        plt.grid(False)

        plt.xticks(range(len(self.slices)), range(len(self.slices)))  
        plt.xlabel('Training set slices')
        plt.ylabel('Competitive ratio')
        plt.title(f'Scheduling on {dataset_name}, sizes: Test = {self.data_loader.test_size} | Train = {self.data_loader.training_size}')
        plt.savefig(filename)



