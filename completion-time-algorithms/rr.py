from job_class import Job
from scientific_not import sci_notation
from tqdm import tqdm


class RR_scheduler:
    def __init__(self):
        self.queue = []
        self.total_completion_time = 0
        self.quantum = 0.01

    def add_job(self, job):
        self.queue.append(job)

    def add_job_set(self, jobset):
        for job in jobset:
            self.add_job(job)

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

    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)
