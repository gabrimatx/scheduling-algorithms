from scheduler_generic import Scheduler
from math import log2, ceil
from scheduler_rr import RR_scheduler
from random import choices
from random import seed
from collections import defaultdict
from tqdm import tqdm
from copy import deepcopy
import numpy as np

class NCS_scheduler(Scheduler):
    def __init__(self, oracle, epsilon):
        self.epsilon = epsilon
        super().__init__()
        self.jobs = []
        self.oracle = oracle

    def add_job_set(self, jobset):
        self.jobs = []
        for idx, job in enumerate(jobset):
            self.jobs.append(job)
            self.queue.append(idx)

    def sort_jobs(self, job):
        prediction = max(self.oracle.getJobPrediction(self.jobs[job]), 0)
        self.jobs[job].prediction = prediction
        return prediction

    def process_job(self, job_idx: int, amount: float) -> bool:
        job = self.jobs[job_idx]
        job.prediction = max(job.prediction - amount, 0)
        if job.remaining_duration <= amount:
            # If job is completed, update objective function and blank it
            self.current_time += job.remaining_duration
            self.jobs[job_idx] = None
            self.total_completion_time += self.current_time
            return 1
        else:
            # Else update its duration and time passed
            job.remaining_duration -= amount
            self.current_time += amount
            return 0
    
    def median_estimator(self, delta, n):
        # Sample the indexes of the jobs, and compute their occurrences to perform a weighted round robin
        sample_size = ceil((log2(2 * n) / (delta**2)))
        sample = choices(self.queue, k=sample_size)
        occurrences = defaultdict(int)
        for idx in sample:
            occurrences[idx] += 1

        # Remove duplicates and sort jobs by order of completion
        sample = list(set(sample))
        sample.sort(key=lambda x: self.jobs[x].remaining_duration / occurrences[x])

        # Compute how much round robin up until half the jobs complete
        finished = idx = rr_per_job = 0
        last_job_time = 0
        completed_jobs = set()
        while 2 * finished < sample_size:
            # Complete first half of the jobs that will finish using round robin
            job = self.jobs[sample[idx]]
            to_process = job.remaining_duration - (rr_per_job * occurrences[sample[idx]])

            # Update round robin processed time and objective function
            rr_per_job += to_process / occurrences[sample[idx]]
            self.current_time += (to_process / occurrences[sample[idx]]) * (sample_size - finished)
            self.total_completion_time += self.current_time

            # Save the job duration and blank it, save it to completed jobs
            last_job_time = job.remaining_duration
            self.jobs[sample[idx]] = None
            finished += occurrences[sample[idx]]
            completed_jobs.add(sample[idx])
            idx += 1
        while idx < len(sample):
            # Process rest of the jobs for the round robin processed time
            job = self.jobs[sample[idx]]
            job.remaining_duration -= rr_per_job * occurrences[sample[idx]]
            assert job.remaining_duration >= 0
            job.prediction = max(job.prediction - rr_per_job * occurrences[sample[idx]], 0)
            idx += 1
        
        # Remove completed jobs from the queue
        self.queue = list(filter(lambda x: self.jobs[x], self.queue))
        return last_job_time

    def error_estimator(self, n, median_k):
        # Collect sample from (i, j) family type
        sample_size = ceil(log2(n) / (self.epsilon**2))
        if not self.queue:
            return 0
        indices = []
        for idx_1, job_idx_1 in enumerate(self.queue):
            for idx_2, job_idx_2 in enumerate(self.queue[idx_1:]):
                indices.append((job_idx_1, job_idx_2))

        sample = choices(indices, k=sample_size)

        # Flatten the sample to compute the metric one time also for duplicate jobs
        job_sample = sorted(set(i for pair in sample for i in pair))

        # Compute the d metric for all the sampled jobs
        d_vector = [0.0 for job in self.jobs]
        max_amount = (1 + self.epsilon) * median_k
        for job_idx in job_sample:
            job = self.jobs[job_idx]
            d_vector[job_idx] = abs(min(job.remaining_duration, max_amount) - min(job.prediction, max_amount))
            self.process_job(job_idx, max_amount)

        # Compute the estimated error
        d_sum = 0
        for pair in sample:
            d_sum += min(d_vector[pair[0]], d_vector[pair[1]])
        self.queue = list(filter(lambda x: self.jobs[x], self.queue))
        return len(indices) * d_sum / sample_size

    def run(self):
        true_jobs = deepcopy(self.jobs)
        true_queue = deepcopy(self.queue)
        results = []
        seed(22)
        # Run the algorithm ten times due to randomness
        for i in tqdm(range(10), desc="NCS Processing"):
            self.current_time = 0
            self.total_completion_time = 0
            self.jobs = deepcopy(true_jobs)
            self.queue = deepcopy(true_queue)
            self.random_run()
            results.append(self.total_completion_time)
        self.total_completion_time = np.mean(results)
        self.std = results

    def random_run(self):
        round = 1
        n = len(self.queue)
        self.queue.sort(key=lambda x: self.sort_jobs(x))
        delta = 1 / 50
        while len(self.queue) >= log2(n) / self.epsilon**3:
            median_k = self.median_estimator(delta, n)
            error_k = self.error_estimator(n, median_k)
            if error_k >= self.epsilon * (delta**2) * median_k * (len(self.queue) ** 2) / 16:
                # Big error, round robin approach
                idx = 0
                while idx < len(self.queue):
                    if self.process_job(self.queue[idx], 2 * median_k):
                        self.queue.pop(idx)
                    else:
                        idx += 1
            else:
                # Small error, greedy approach
                idx = 0
                self.queue.sort(key=lambda x: self.jobs[x].prediction)
                while idx < len(self.queue):
                    if self.jobs[self.queue[idx]].prediction <= (1 + self.epsilon) * median_k:
                        if self.process_job(self.queue[idx], self.jobs[self.queue[idx]].prediction + 3 * self.epsilon * median_k):
                            self.queue.pop(idx)
                        else:
                            idx += 1
                    else:
                        break
            round += 1
        # Finish remaining jobs with round robin
        finisher = RR_scheduler()
        finisher.add_job_set(filter(lambda x: x is not None, self.jobs))
        finisher.current_time = self.current_time
        finisher.run()
        self.total_completion_time += finisher.total_completion_time



class DNCS_scheduler(Scheduler):
    def __init__(self, oracle, epsilon):
        self.epsilon = epsilon
        super().__init__()
        self.jobs = []
        self.oracle = oracle

    def add_job_set(self, jobset):
        self.jobs = []
        for idx, job in enumerate(jobset):
            self.jobs.append(job)
            self.queue.append(idx)

    def sort_jobs(self, job):
        prediction = max(self.oracle.getJobPrediction(self.jobs[job]), 0)
        self.jobs[job].prediction = prediction
        return prediction

    def process_job(self, job_idx: int, amount: float) -> bool:
        job = self.jobs[job_idx]
        job.prediction = max(job.prediction - amount, 0)
        if job.remaining_duration <= amount:
            # If job is completed, update objective function and blank it
            self.current_time += job.remaining_duration
            self.oracle.updatePrediction_NH(self.jobs[job_idx]) # Update predictions dynamically
            self.jobs[job_idx] = None
            self.total_completion_time += self.current_time
            return 1
        else:
            # Else process job for the amount and update time passed
            job.remaining_duration -= amount
            self.current_time += amount
            return 0
    
    def median_estimator(self, delta, n):
        # Sample the indexes of the jobs, and compute their occurrences to perform a weighted round robin
        sample_size = ceil((log2(2 * n) / (delta**2)))
        sample = choices(self.queue, k=sample_size)
        occurrences = defaultdict(int)
        for idx in sample:
            occurrences[idx] += 1

        # Remove duplicates and sort jobs by order of completion
        sample = list(set(sample))
        sample.sort(key=lambda x: self.jobs[x].remaining_duration / occurrences[x])

        # Compute how much round robin up until half the jobs complete
        finished = idx = rr_per_job = 0
        last_job_time = 0
        completed_jobs = set()
        while 2 * finished < sample_size:
            # Complete first half of the jobs that will finish using round robin
            job = self.jobs[sample[idx]]
            to_process = job.remaining_duration - (rr_per_job * occurrences[sample[idx]])

            # Update round robin processed time
            rr_per_job += to_process / occurrences[sample[idx]]
            self.current_time += (to_process / occurrences[sample[idx]]) * (sample_size - finished)
            self.total_completion_time += self.current_time
            last_job_time = job.remaining_duration
            self.oracle.updatePrediction_NH(self.jobs[sample[idx]]) # Update predictions dynamically

            # Blank the job and update completed jobs
            self.jobs[sample[idx]] = None
            finished += occurrences[sample[idx]]
            completed_jobs.add(sample[idx])
            idx += 1
        while idx < len(sample):
            # Process rest of the jobs for the round robin processed time
            job = self.jobs[sample[idx]]
            job.remaining_duration -= rr_per_job * occurrences[sample[idx]]
            assert job.remaining_duration >= 0
            job.prediction = max(job.prediction - rr_per_job * occurrences[sample[idx]], 0)
            idx += 1
        
        # Remove completed jobs from the queue
        self.queue = list(filter(lambda x: self.jobs[x], self.queue))
        return last_job_time

    def error_estimator(self, n, median_k):
        # Collect sample from (i, j) family type
        sample_size = ceil(log2(n) / (self.epsilon**2))
        if not self.queue:
            return 0
        indices = []
        for idx_1, job_idx_1 in enumerate(self.queue):
            for idx_2, job_idx_2 in enumerate(self.queue[idx_1:]):
                indices.append((job_idx_1, job_idx_2))

        sample = choices(indices, k=sample_size)

        # Flatten the sample to compute the metric one time also for duplicate jobs
        job_sample = sorted(set(i for pair in sample for i in pair))

        # Compute the d metric for all the sampled jobs
        d_vector = [0.0 for job in self.jobs]
        max_amount = (1 + self.epsilon) * median_k
        for job_idx in job_sample:
            job = self.jobs[job_idx]
            d_vector[job_idx] = abs(min(job.remaining_duration, max_amount) - min(job.prediction, max_amount))
            self.process_job(job_idx, max_amount)

        # Compute the estimated error
        d_sum = 0
        for pair in sample:
            d_sum += min(d_vector[pair[0]], d_vector[pair[1]])
        self.queue = list(filter(lambda x: self.jobs[x], self.queue))
        return len(indices) * d_sum / sample_size

    def run(self):
        true_jobs = deepcopy(self.jobs)
        true_queue = deepcopy(self.queue)
        initial_oracle = deepcopy(self.oracle)
        results = []
        seed(22)
        # Run the algorithm ten times due to randomness
        for i in tqdm(range(10), desc="DNCS Processing"):
            self.current_time = 0
            self.total_completion_time = 0
            self.jobs = deepcopy(true_jobs)
            self.queue = deepcopy(true_queue)
            self.oracle = deepcopy(initial_oracle) # Reset training set to avoid using test set jobs
            self.random_run()
            results.append(self.total_completion_time)
        self.total_completion_time = np.mean(results)
        self.std = results

    def random_run(self):
        round = 1
        n = len(self.queue)
        delta = 1 / 50
        while len(self.queue) >= log2(n) / self.epsilon**3:
            median_k = self.median_estimator(delta, n)

            # Sort jobs each round to update predictions for error estimator
            self.queue.sort(key=lambda x: self.sort_jobs(x))

            error_k = self.error_estimator(n, median_k)

            # Sort jobs after error estimation
            self.queue.sort(key=lambda x: self.sort_jobs(x))

            if error_k >= self.epsilon * (delta**2) * median_k * (len(self.queue) ** 2) / 16:
                # Big error, round robin approach
                idx = 0
                while idx < len(self.queue):
                    if self.process_job(self.queue[idx], 2 * median_k):
                        self.queue.pop(idx)
                    else:
                        idx += 1
            else:
                # Small error, greedy approach
                idx = 0
                while idx < len(self.queue):
                    if self.jobs[self.queue[idx]].prediction <= (1 + self.epsilon) * median_k:
                        if self.process_job(self.queue[idx], self.jobs[self.queue[idx]].prediction + 3 * self.epsilon * median_k):
                            self.queue.pop(idx)
                            # Sort job after the predictions have been updated again
                            self.queue.sort(key=lambda x: self.sort_jobs(x))
                        else:
                            idx += 1
                    else:
                        break
            round += 1
        # Finish remaining jobs with round robin
        finisher = RR_scheduler()
        finisher.add_job_set(filter(lambda x: x is not None, self.jobs))
        finisher.current_time = self.current_time
        finisher.run()
        self.total_completion_time += finisher.total_completion_time
