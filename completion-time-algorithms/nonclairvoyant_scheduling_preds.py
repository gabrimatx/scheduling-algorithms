from oracles import *
from job_class import Job
import random
import copy
import math
from scientific_not import sci_notation
import time

class NCS_scheduler:
    def __init__(self, time_quantum, oracle, epsilon):
        self.queue = []
        self.total_completion_time = 0
        self.k = 0
        self.delta = 1/50
        self.epsilon = epsilon
        self.quantum = time_quantum
        self.n = 0
        self.current_time = 0
        self.oracle = oracle

    def add_job(self, job):
        self.queue.append(job)

    def oracle_predict(self, job):
        prediction = max(0, self.oracle.getJobPrediction(job)) # TODO: Find how to fix this
        job.oracle_prediction = prediction
        return prediction

    def median_estimator(self):
        # Generate sample S 
        S = random.choices([i for i in range(len(self.queue))], k = math.ceil(math.log(2 * self.n) / (self.delta**2)))
        jobs_to_finish = len(S) // 2 
        completed_jobs = set()
        completed_S_elements = set()
        last_job_duration = 1
        while len(completed_S_elements) < jobs_to_finish:
            job_ind = 0

            if self.queue:
                remaining_sizes = [job.remaining_duration for job in [self.queue[j_ind] for j_ind in S if self.queue[j_ind].remaining_duration > 0]]
                if remaining_sizes:
                    minimum_round_size = min(remaining_sizes)
                else:
                    minimum_round_size = 0
                    
                if minimum_round_size > self.quantum:
                    round_quantum = ((minimum_round_size) // self.quantum) * self.quantum
                else:
                    round_quantum = self.quantum

            round_quantum = self.quantum
            while job_ind < len(S):
                if S[job_ind] in completed_jobs:
                    completed_S_elements.add(job_ind)
                    job_ind += 1
                    continue
                if round_quantum >= self.queue[S[job_ind]].remaining_duration:
                    last_job_duration = self.queue[S[job_ind]].remaining_duration
                    self.current_time += last_job_duration
                    self.total_completion_time += self.current_time
                    self.queue[S[job_ind]].remaining_duration = 0
                    completed_jobs.add(S[job_ind])
                    completed_S_elements.add(job_ind)
                else:
                    self.queue[S[job_ind]].remaining_duration -= round_quantum
                    self.current_time += round_quantum
                job_ind += 1
        for finished_job in sorted(list(completed_jobs), reverse=True):
            self.queue.pop(finished_job)
        # print(f"lastjd {last_job_duration}")
        return last_job_duration

    def error_estimator(self, estimated_median_k):
        # Construct the Q family 
        Q = [(J, J) for J in self.queue]
        for job_index in range(len(self.queue)):
            for second_job_index in range(job_index):
                Q += [tuple([self.queue[second_job_index], self.queue[job_index]])]

        if not Q:
            return 0
        P = random.choices(Q, k = math.ceil((1 / (self.epsilon ** 2)) * math.log10(self.n)))
        sum_of_estimations = 0
        median_time_running = (1 + self.epsilon) * estimated_median_k
        for job_pair in P:
            # running simulation is avoided to speed up the metric estimation 
            d_k_j = abs(min(median_time_running, job_pair[1].remaining_duration) - min(median_time_running, job_pair[1].oracle_prediction - (job_pair[1].real_duration - job_pair[1].remaining_duration)))
            d_k_i = abs(min(median_time_running, job_pair[0].remaining_duration) - min(median_time_running, job_pair[0].oracle_prediction - (job_pair[0].real_duration - job_pair[0].remaining_duration)))
            sum_of_estimations += min(d_k_i, d_k_j)
        return sum_of_estimations * len(Q) / len(P)

    def run(self):
        self.n = len(self.queue)
        self.queue.sort(key = lambda j: self.oracle_predict(j))
        greedy_completed_moves = 0
        round_robin_rounds = 0
        time_in_median = 0
        time_in_error = 0


        while len(self.queue) > math.ceil(1 / (self.epsilon**3) * math.log10(self.n)):
            self.k += 1

            m_b = time.time()
            round_median = self.median_estimator()
            m_a = time.time()
            round_error = self.error_estimator(round_median)
            e_a = time.time()

            time_in_median += m_a - m_b
            time_in_error += e_a - m_a
            if round_error >= (self.delta**2 * self.epsilon * round_median * len(self.queue)**2) / 16:
                # Big error, run round robin for 2*round_median
                round_robin_rounds += 1
                if round_median == 0:
                    print("GRAVE")
                job_ind = 0
                while job_ind < len(self.queue):
                    if self.queue[job_ind].remaining_duration > 2 * round_median:
                        self.queue[job_ind].remaining_duration -= 2 * round_median
                        self.current_time += 2 * round_median
                    else:
                        self.current_time += self.queue[job_ind].remaining_duration
                        self.queue.pop(job_ind)
                        self.total_completion_time += self.current_time
                        job_ind += 1
            else:
                # Small error, greedy approach
                self.queue.sort(key = lambda x: x.oracle_prediction - (x.real_duration - x.remaining_duration)) # Implement heap for better efficiency
                job_ind = 0
                while job_ind < len(self.queue):
                    remaining_time_estimate = max(0, self.queue[job_ind].oracle_prediction - (self.queue[job_ind].real_duration - self.queue[job_ind].remaining_duration))
                    if remaining_time_estimate <= (1 + self.epsilon) * round_median:
                        greedy_completed_moves += 1
                        if remaining_time_estimate + 3*self.epsilon*round_median < 0:
                            print(f"AAAAAAAAAAAAAAAAAAAAAAAAA {remaining_time_estimate}")
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

        # Finish with round robin
        # print(len(self.queue), self.k, f"rr round  {round_robin_rounds} greedy round {self.k - round_robin_rounds}")
        # print(f"time in median estimation {time_in_median}\n time in error estimation {time_in_error}")
        while self.queue:
            # Run the scheduler in round robin fashion
            job_ind = 0
            queue_size = len(self.queue)
            while job_ind < queue_size:
                if self.quantum >= self.queue[job_ind].remaining_duration:
                    self.current_time += (self.queue.pop(job_ind)).remaining_duration
                    self.total_completion_time += self.current_time
                    queue_size -= 1
                else:
                    self.current_time += self.quantum
                    self.queue[job_ind].remaining_duration -= self.quantum
                    job_ind += 1

        # print(f"greedy completed jobs {greedy_completed_moves}")

    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)


if __name__ == '__main__':
    
    numjobs = int(input("Insert number of jobs to process: "))
    oracle = JobMeanOracle()
    scheduler = NCS_scheduler(100, oracle, 10)
    filename = r"task_lines.txt"
    with open(filename, "r") as f:
        for i in range(numjobs):
            a = [int(x) for x in f.readline().split(",")]
            scheduler.add_job(Job(a[1], a[0]//1000, a[2]//1000))
    # Running the scheduler
    scheduler.run()
    print(f"total_completion_time: {sci_notation(scheduler.total_completion_time)}")
