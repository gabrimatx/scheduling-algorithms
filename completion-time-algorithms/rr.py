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
        if min_ind < len(ord_jobs):
            minimum_round_size = ord_jobs[min_ind].remaining_duration - 1
        else:
            return 0.001

        if minimum_round_size > self.quantum:
            round_quantum = (minimum_round_size // self.quantum) * self.quantum
        else:
            round_quantum = self.quantum

        return round_quantum

    def run(self):
        current_time = 0
        ordered_jobs = sorted(self.queue)
        min_index = 0
        round_quantum = self.compute_round_quantum(ordered_jobs, min_index)
        job_ind = 0
        with tqdm(total=len(self.queue), desc="Processing (rr)...") as pbar:
            while self.queue:
                job_ind = job_ind % len(self.queue)

                if not job_ind:
                    round_quantum = self.compute_round_quantum(ordered_jobs, min_index)

                if round_quantum >= self.queue[job_ind].remaining_duration:
                    current_time += (self.queue.pop(job_ind)).remaining_duration
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


if __name__ == "__main__":
    scheduler = RR_scheduler()

    # Adding jobs
    numjobs = 10000
    filename = r"task_lines.txt"
    with open(filename, "r") as f:
        for i in range(numjobs):
            a = [int(x) for x in f.readline().split(",")]
            scheduler.add_job(Job(a[1], a[0] // 1000000, a[2] // 1000000))
    # Running the scheduler
    scheduler.run()
    print(f"total_completion_time: {sci_notation(scheduler.total_completion_time)}")
