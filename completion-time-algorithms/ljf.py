from oracles import *
from job_class import Job
from scientific_not import sci_notation
from tqdm import tqdm


class LJF_scheduler:
    def __init__(self):
        self.queue = []
        self.total_completion_time = 0
        self.total_error = 0

    def add_job(self, job):
        self.queue.append(job)

    def add_job_set(self, jobset):
        for job in jobset:
            self.add_job(job)

    def sort_and_add_error(self, job):
        prediction = self.oracle.getJobPrediction(job)
        self.total_error += abs(job.real_duration - prediction)
        return prediction

    def run(self):
        current_time = 0
        self.queue.sort(reverse=True)
        for i in tqdm(range(len(self.queue)), "Processing (lfj)..."):
            job = self.queue[i]
            current_time += job.remaining_duration
            self.total_completion_time += current_time

    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)
