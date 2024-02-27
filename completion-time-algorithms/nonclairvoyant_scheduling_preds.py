from oracles import *
from job_class import Job
import random
import copy
import math

class NCS_scheduler:
    def __init__(self):
        self.queue = []
        self.total_completion_time = 0
        self.k = 1
        self.delta = 1/50
        self.epsilon = 0.1
        self.quantum = 5
        self.n = 0

    def add_job(self, job):
        self.queue.append(job)

    def oracle_predict(self, job):
        prediction = oracle.getJobPrediction(job)
        job.oracle_prediction = prediction
        return prediction

    def median_estimator(self):
        # Generate sample S 
        S = copy.deepcopy(random.choices(self.queue, k = math.ceil(math.log(2 * self.n) / (self.delta**2))))
        jobs_to_finish = len(S) // 2 
        last_job = -1
        while len(S) > jobs_to_finish:
            job_ind = 0
            while job_ind < queue_size:
                if self.quantum >= S[job_ind].remaining_duration:
                    last_job = (S.pop(job_ind)).remaining_duration
                else:
                    S[job_ind].remaining_duration -= self.quantum
                    job_ind += 1
        return last_job

    def error_estimator(self, estimated_median_k):
        # Construct the Q family 
        Q = [(copy.copy(J), copy.copy(J)) for J in self.queue]
        for job_index in range(len(self.queue)):
            for second_job_index in range(job_index):
                Q += [(copy.copy(self.queue[second_job_index]), copy.copy(self.queue[job_index]))]

        P = copy.deepcopy(random.choices(Q, k = math.ceil((1 / (self.epsilon ** 2)) * math.log10(self.n))))
        sum_of_estimations = 0
        median_time_running = (1 + self.epsilon) * estimated_median_k
        for job_pair in P:
            # running simulation is avoided to speed up the metric estimation 
            d_k_j = abs(min(median_time_running, job_pair[1].remaining_duration) - min(median_time_running, job_pair[1].oracle_prediction - (job_pair[1].real_duration - job_pair[1].remaining_duration)))
            d_k_i = abs(min(median_time_running, job_pair[0].remaining_duration) - min(median_time_running, job_pair[0].oracle_prediction - (job_pair[0].real_duration - job_pair[0].remaining_duration)))
            sum_of_estimations += min(d_k_i, d_k_j)
        return sum_of_estimations * len(Q) / len(P)

    def run(self):
        current_time = 0
        self.n = len(self.queue)
        oracle.computePredictions(self.queue[50:])
        self.queue.sort(key = lambda j: self.oracle_predict(j))
        while len(self.queue) > math.ceil(1 / (self.epsilon**3) * math.log10(self.n)):
            round_median = self.median_estimator()
            round_error = self.error_estimator(round_median)
            if round_error >= (self.delta**2 * self.epsilon * self.round_median * len(self.queue)**2) / 16:
                # Big error, run round robin for 2*round_median
                for job_ind in range(len(self.queue)):
                    if self.queue[job_ind].remaining_duration > 2 * round_median:
                        self.queue[job_ind].remaining_duration -= 2 * round_median
                        current_time += 2 * round_median
                    else:
                        current_time += self.queue[job_ind].remaining_duration
                        self.queue.pop(job_ind)
                        self.total_completion_time += current_time
            else:
                # Small error, greedy approach
                self.queue.sort(key = lambda x: x.oracle_prediction - (x.real_duration - x.remaining_duration)) # Implement heap for better efficiency
                for job_ind in range(len(self.queue)):
                    remaining_time_estimate = self.queue[job_ind].oracle_prediction - (self.queue[job_ind].real_duration - self.queue[job_ind].remaining_duration)
                    if remaining_time_estimate <= (1 + self.epsilon) * round_median:
                        if self.queue[job_ind].remaining_duration <= remaining_time_estimate + 3*self.epsilon*round_median:
                            current_time += self.queue[job_ind].remaining_duration
                            self.queue.pop(job_ind)
                            self.total_completion_time += current_time
                        else:
                            self.queue[job_ind].remaining_duration -= remaining_time_estimate + 3*self.epsilon*round_median
                            current_time += remaining_time_estimate + 3*self.epsilon*round_median
                    else:
                        break

        # Finish with round robin
        while self.queue:
            # Run the scheduler in round robin fashion
            job_ind = 0
            queue_size = len(self.queue)
            while job_ind < queue_size:
                if self.quantum >= self.queue[job_ind].remaining_duration:
                    current_time += (self.queue.pop(job_ind)).remaining_duration
                    self.total_completion_time += current_time
                    queue_size -= 1
                else:
                    current_time += self.quantum
                    self.queue[job_ind].remaining_duration -= self.quantum
                    job_ind += 1

    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)


if __name__ == '__main__':
    scheduler = NCS_scheduler()
    numjobs = int(input("Insert number of jobs to process: "))
    oracle = JobMeanOracle()
    filename = r"task_lines.txt"
    with open(filename, "r") as f:
        for i in range(numjobs):
            a = [int(x) for x in f.readline().split(",")]
            scheduler.add_job(Job(a[1], a[0]//1000, a[2]//1000))
    # Running the scheduler
    scheduler.run()
    print(f"total_completion_time: {scheduler.total_completion_time}")
