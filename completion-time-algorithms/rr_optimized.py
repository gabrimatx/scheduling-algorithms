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

    def run(self):
        current_time = 0
        processed_time = 0
        ordered_jobs = sorted(self.queue)
        min_index = 0
        with tqdm(total=len(self.queue), desc="Processing (rr)...") as pbar:
            while min_index < len(ordered_jobs):
                time_to_pass = ordered_jobs[min_index].remaining_duration - processed_time
                processed_time += time_to_pass
                current_time += time_to_pass * (len(ordered_jobs) - min_index)
                self.total_completion_time += current_time
                min_index += 1
                pbar.update(1)


    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)

if __name__ == "__main__":
    scheduler = RR_scheduler()

    # Adding jobs
    numjobs = 50
    filename = r"task_lines.txt"
    with open(filename, "r") as f:
        for i in range(numjobs):
            a = [int(x) for x in f.readline().split(",")]
            scheduler.add_job(Job(a[1], a[0] // 1000000, a[2] // 1000000))
    # Running the scheduler
    scheduler.run()
    print(f"total_completion_time: {sci_notation(scheduler.total_completion_time)}")
