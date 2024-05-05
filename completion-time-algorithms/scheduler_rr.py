from tqdm import tqdm
from scheduler_generic import Scheduler


class RR_scheduler(Scheduler):
    def __init__(self):
        super().__init__()

    def run(self):
        rr_time = 0
        ordered_jobs = sorted(self.queue)
        min_index = 0
        with tqdm(total=len(self.queue), desc="Processing - RR", disable=True) as pbar:
            while min_index < len(ordered_jobs):
                # Compute how much time we are gonna need to complete the next job
                # (taking into consideration how much the round robin scheduler has already processed it)
                time_left = ordered_jobs[min_index].remaining_duration - rr_time
                rr_time += time_left

                # Update objective function and move on to the next job
                self.current_time += time_left * (len(ordered_jobs) - min_index)
                self.total_completion_time += self.current_time
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
        # Round robin without the relaxing assumption of continuous time
        # (Used to assert whether computations are correct for low number of jobs)
        ordered_jobs = sorted(self.queue)
        min_index = 0
        round_quantum = self.compute_round_quantum(ordered_jobs, min_index)
        job_ind = 0
        with tqdm(total=len(self.queue), desc="Processing - RRnaive") as pbar:
            while min_index < len(ordered_jobs):
                job_ind = job_ind % len(self.queue)

                if not job_ind:
                    round_quantum = self.compute_round_quantum(ordered_jobs, min_index)

                if self.queue[job_ind].remaining_duration == 0:
                    job_ind += 1
                    continue

                if round_quantum >= self.queue[job_ind].remaining_duration:
                    self.current_time += self.queue[job_ind].remaining_duration
                    self.queue[job_ind].remaining_duration = 0
                    pbar.update(1)
                    self.total_completion_time += self.current_time
                    min_index += 1
                else:
                    self.current_time += round_quantum
                    self.queue[job_ind].remaining_duration -= round_quantum
                    job_ind += 1
