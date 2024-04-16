from tqdm import tqdm
import random
from scheduler_generic import Scheduler


class RAND_scheduler(Scheduler):
    def __init__(self):
        super().__init__()
        random.seed(22)

    def add_job(self, job):
        self.queue.append(job)

    def run(self):
        current_time = 0
        random.shuffle(self.queue)
        for i in tqdm(range(len(self.queue)), "Processing (rand)..."):
            job = self.queue[i]
            current_time += job.remaining_duration
            self.total_completion_time += current_time


class LJF_scheduler(Scheduler):
    def run(self):
        current_time = 0
        self.queue.sort(reverse=True)
        for i in tqdm(range(len(self.queue)), "Processing (ljf)..."):
            job = self.queue[i]
            current_time += job.remaining_duration
            self.total_completion_time += current_time
