from oracles import *
from job_class import Job
from scientific_not import sci_notation
from my_heap import HeapWithJobs
from tqdm import tqdm
import time


class PRR_scheduler:
    def __init__(self, lambda_parameter, oracle):
        self.queue = []
        self.total_completion_time = 0
        self.hyperLambda = lambda_parameter
        self.total_error = 0
        self.oracle = oracle

    def add_job_set(self, jobset):
        for job in jobset:
            self.add_job(job)

    def add_job(self, job):
        self.queue.append(job)

    def sort_and_add_error(self, job):
        prediction = self.oracle.getJobPrediction(job)
        self.total_error += abs(job.real_duration - prediction)
        return prediction

    def run(self):
        current_time = 0
        min_job_heap = HeapWithJobs(self.queue)
        self.queue.sort(key=lambda j: self.sort_and_add_error(j))
        round_robin_processed_time = 0
        completed_jobs = [False for job in self.queue]
        spjf_index = 0
        completed_count = 0
        time_for_rr = self.hyperLambda
        time_for_spjf = 1 - self.hyperLambda
        for index, job in tqdm(
            enumerate(self.queue),
            total=len(self.queue),
            desc="Enumerating jobs (prr)...",
        ):
            job.queue_index = index

        with tqdm(total=len(self.queue), desc="Processing (prr)...") as pbar:
            while len(self.queue) > completed_count:
                remaining_jobs = max(len(self.queue) - completed_count, 1)
                while completed_jobs[spjf_index]:
                    spjf_index += 1
                    continue
                pbar.update(1)
                if self.queue[spjf_index] is min_job_heap.get_top():
                    rounds_to_complete = (
                        self.queue[spjf_index].remaining_duration
                        - round_robin_processed_time
                    ) / (time_for_spjf + time_for_rr / remaining_jobs)
                    processing_time = (
                        time_for_rr / remaining_jobs
                    ) * rounds_to_complete
                    round_robin_processed_time += processing_time
                    current_time += rounds_to_complete
                    self.total_completion_time += current_time

                    completed_jobs[spjf_index] = True
                    completed_count += 1
                    self.queue[spjf_index].remaining_duration = float("inf")
                    min_job_heap.heapify(self.queue[spjf_index].heap_index)
                    spjf_index += 1

                else:
                    rounds_to_complete_predicted = (
                        self.queue[spjf_index].remaining_duration
                        - round_robin_processed_time
                    ) / (time_for_spjf + time_for_rr / remaining_jobs)

                    rounds_to_complete_smallest = (
                        min_job_heap.get_top().remaining_duration
                        - round_robin_processed_time
                    ) / (time_for_rr / remaining_jobs)

                    if rounds_to_complete_predicted < rounds_to_complete_smallest:
                        processing_time = (
                            time_for_rr / remaining_jobs
                        ) * rounds_to_complete_predicted

                        round_robin_processed_time += processing_time
                        current_time += rounds_to_complete_predicted
                        self.total_completion_time += current_time

                        completed_jobs[spjf_index] = True
                        completed_count += 1
                        self.queue[spjf_index].remaining_duration = float("inf")
                        min_job_heap.heapify(self.queue[spjf_index].heap_index)
                        spjf_index += 1
                    else:
                        min_job_heap.process_job(
                            self.queue[spjf_index].heap_index,
                            time_for_spjf * rounds_to_complete_smallest,
                        )

                        completed_job = min_job_heap.get_top()

                        processing_time = (
                            time_for_rr / remaining_jobs
                        ) * rounds_to_complete_smallest

                        round_robin_processed_time += processing_time
                        current_time += rounds_to_complete_smallest
                        self.total_completion_time += current_time

                        completed_jobs[completed_job.queue_index] = True
                        completed_count += 1
                        self.queue[completed_job.queue_index].remaining_duration = (
                            float("inf")
                        )
                        min_job_heap.heapify(
                            self.queue[completed_job.queue_index].heap_index
                        )

    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)
