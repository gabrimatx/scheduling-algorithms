from tqdm import tqdm
from scheduler_generic import Scheduler

class RR_scheduler(Scheduler):
    def __init__(self):
        super().__init__()

    def run(self):
        current_time = 0
        processed_time = 0
        ordered_jobs = sorted(self.queue)
        min_index = 0
        with tqdm(total=len(self.queue), desc="Processing (rr)...") as pbar:
            while min_index < len(ordered_jobs):
                time_to_pass = (
                    ordered_jobs[min_index].remaining_duration - processed_time
                )
                processed_time += time_to_pass
                current_time += time_to_pass * (len(ordered_jobs) - min_index)
                self.total_completion_time += current_time
                min_index += 1
                pbar.update(1)

class RR_naive_scheduler(Scheduler):
    def __init__(self):
        super().__init__()
        self.quantum = 0.01

    def compute_round_quantum(self, ord_jobs, min_ind):
        minimum_round_size = ord_jobs[min_ind].remaining_duration
        round_quantum = max(
            minimum_round_size // self.quantum * self.quantum, self.quantum
        )
        return round_quantum

    def run(self):
        current_time = 0
        ordered_jobs = sorted(self.queue)
        min_index = 0
        round_quantum = self.compute_round_quantum(ordered_jobs, min_index)
        job_ind = 0
        with tqdm(total=len(self.queue), desc="Processing (rr)...") as pbar:
            while min_index < len(ordered_jobs):
                job_ind = job_ind % len(self.queue)

                if not job_ind:
                    round_quantum = self.compute_round_quantum(ordered_jobs, min_index)

                if self.queue[job_ind].remaining_duration == 0:
                    job_ind += 1
                    continue

                if round_quantum >= self.queue[job_ind].remaining_duration:
                    current_time += self.queue[job_ind].remaining_duration
                    self.queue[job_ind].remaining_duration = 0
                    pbar.update(1)
                    self.total_completion_time += current_time
                    min_index += 1
                else:
                    current_time += round_quantum
                    self.queue[job_ind].remaining_duration -= round_quantum
                    job_ind += 1
