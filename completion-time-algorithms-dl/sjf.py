from job_class import Job
from scientific_not import sci_notation
from tqdm import tqdm


class SJF_scheduler:
    def __init__(self):
        self.queue = []
        self.total_completion_time = 0

    def add_job_set(self, jobset):
        for job in jobset:
            self.add_job(job)

    def add_job(self, job):
        self.queue.append(job)

    def run(self):
        current_time = 0
        self.queue = sorted(self.queue)
        for job in tqdm(self.queue, total=len(self.queue), desc="Processing (sjf)..."):
            current_time += job.remaining_duration
            self.total_completion_time += current_time

    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)
