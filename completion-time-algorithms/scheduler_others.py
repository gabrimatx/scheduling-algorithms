from tqdm import tqdm
import random
from scheduler_generic import Scheduler


# A scheduler that executes job randomly, and one that executes them in non-incresing order (For evaluation purposes)


class RAND_scheduler(Scheduler):
    def __init__(self):
        super().__init__()
        random.seed(22)

    def run(self):
        random.shuffle(self.queue)
        for job in tqdm(self.queue, total=len(self.queue), desc="Processing - RAND"):
            self.current_time += job.remaining_duration
            self.total_completion_time += self.current_time


class LJF_scheduler(Scheduler):
    def run(self):
        self.queue.sort(reverse=True)
        for job in tqdm(self.queue, total=len(self.queue), desc="Processing - LJF"):
            self.current_time += job.remaining_duration
            self.total_completion_time += self.current_time
