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
        random.shuffle(self.queue)
        for i in tqdm(range(len(self.queue)), "Processing (rand)..."):
            job = self.queue[i]
            self.current_time += job.remaining_duration
            self.total_completion_time += self.current_time


class LJF_scheduler(Scheduler):
    def run(self):
        self.queue.sort(reverse=True)
        for i in tqdm(range(len(self.queue)), "Processing (ljf)..."):
            job = self.queue[i]
            self.current_time += job.remaining_duration
            self.total_completion_time += self.current_time
