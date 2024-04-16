from oracles import *
from job_class import Job
import random
import copy
import math
from tqdm import tqdm
import time
random.seed(20)
class NCS_scheduler:
    def __init__(self, oracle, epsilon):
        self.queue = []
        self.total_completion_time = 0
        self.delta = 1/50
        self.epsilon = epsilon
        self.n = 0
        self.current_time = 0
        self.oracle = oracle

    def add_job(self, job):
        self.queue.append(job)

    def add_job_set(self, jobset):
        for job in jobset:
            self.add_job(job)

    def oracle_predict(self, job):
        prediction = self.oracle.getJobPrediction(job)
        job.oracle_prediction = prediction
        return prediction

    def median_estimator(self):
        S = random.choices([i for i in range(len(self.queue))], k = math.ceil(math.log(2 * self.n) / (self.delta**2)))
        occurrences = {}
        for index in S:
            occurrences[index] = occurrences.get(index, 0) + 1
        completion_order = sorted(list(occurrences.keys()), key=lambda x: self.queue[x].remaining_duration / occurrences[x])
        jobs_to_finish = len(S) // 2
        finished_jobs = 0
        index = 0
        processing_time = 0
        # Simulate a round robin with continuous time, improve performance
        while finished_jobs < jobs_to_finish:
            job_index = completion_order[index]
            job_occurrences, job = occurrences[job_index], self.queue[job_index]
            processing_time += (job.remaining_duration - (processing_time * job_occurrences)) / job_occurrences
            self.current_time += processing_time * (len(S) - finished_jobs)
            self.total_completion_time += self.current_time
            finished_jobs += job_occurrences
            index += 1
        
        median_est = job.remaining_duration
        completed_jobs, uncomplete_jobs = completion_order[:index], completion_order[index:]

        for job_index in uncomplete_jobs:
            job = self.queue[job_index]
            job.remaining_duration -= processing_time * occurrences[job_index]
            
        for job_index in sorted(completed_jobs, reverse=True):
            self.queue.pop(job_index)
        
        return median_est
        
    def error_estimator(self, estimated_median_k):
        Q = [(J, J) for J in range(len(self.queue))]
        for job_index in range(len(self.queue)):
            for second_job_index in range(job_index):
                Q += [tuple([second_job_index, job_index])]
        if not Q:
            return 0
        P = random.choices(Q, k = math.ceil((1 / (self.epsilon ** 2)) * math.log10(self.n)))
        median_time_running = (1 + self.epsilon) * estimated_median_k
        d_list = []
        completed_jobs = set()
        for job_pair in P:
            # running simulation is avoided to speed up the metric estimation
            min_d = float('inf')
            for j_ind in job_pair:
                d_k = abs(min(median_time_running, self.queue[j_ind].remaining_duration) - min(median_time_running, max(self.queue[j_ind].oracle_prediction - (self.queue[j_ind].real_duration - self.queue[j_ind].remaining_duration), 0)))
                if median_time_running >= self.queue[j_ind].remaining_duration and j_ind not in completed_jobs:
                    self.current_time += self.queue[j_ind].remaining_duration
                    self.total_completion_time += self.current_time
                    self.queue[j_ind].remaining_duration = 0
                    completed_jobs.add(j_ind)
                elif j_ind not in completed_jobs:
                    self.current_time += median_time_running
                    self.queue[j_ind].remaining_duration -= median_time_running
                min_d = min(d_k, min_d)
            d_list.append(min_d)
        for finished_job in sorted(list(completed_jobs), reverse=True):
            self.queue.pop(finished_job)
        return sum(d_list) * len(Q) / len(P)

    def run(self):
        self.n = len(self.queue)
        self.queue.sort(key = lambda j: self.oracle_predict(j))
        total_length = len(self.queue)
        pbar = tqdm(total=total_length, desc="Processing NCS...")
        while len(self.queue) > math.ceil(1 / (self.epsilon**3) * math.log10(self.n)):
            pbar.update(total_length - len(self.queue))
            total_length -= total_length - len(self.queue)
            round_median = self.median_estimator()
            round_error = self.error_estimator(round_median)
            if round_error >= (self.delta**2 * self.epsilon * round_median * len(self.queue)**2) / 16:
                # Big error, run round robin for 2*round_median
                job_ind = 0
                while job_ind < len(self.queue):
                    if self.queue[job_ind].remaining_duration > 2 * round_median:
                        self.queue[job_ind].remaining_duration -= 2 * round_median
                        self.current_time += 2 * round_median
                        job_ind += 1
                    else:
                        self.current_time += self.queue[job_ind].remaining_duration
                        self.queue.pop(job_ind)
                        self.total_completion_time += self.current_time
            else:
                # Small error, greedy approach
                job_ind = 0
                while job_ind < len(self.queue):
                    remaining_time_estimate = max(0, self.queue[job_ind].oracle_prediction - (self.queue[job_ind].real_duration - self.queue[job_ind].remaining_duration))
                    if remaining_time_estimate <= (1 + self.epsilon) * round_median:
                        if self.queue[job_ind].remaining_duration <= remaining_time_estimate + 3*self.epsilon*round_median:
                            self.current_time += self.queue[job_ind].remaining_duration
                            self.queue.pop(job_ind)
                            self.total_completion_time += self.current_time
                        else:
                            self.queue[job_ind].remaining_duration -= remaining_time_estimate + 3*self.epsilon*round_median
                            self.current_time += remaining_time_estimate + 3*self.epsilon*round_median
                            job_ind += 1
                    else:
                        break
        pbar.update(total_length - len(self.queue))
        self.queue.sort()
        min_index = 0
        processed_time = 0
        while min_index < len(self.queue):
            time_to_pass = self.queue[min_index].remaining_duration - processed_time
            processed_time += time_to_pass
            self.current_time += time_to_pass * (len(self.queue) - min_index)
            self.total_completion_time += self.current_time
            min_index += 1
            pbar.update(1)
        pbar.close()

    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)

