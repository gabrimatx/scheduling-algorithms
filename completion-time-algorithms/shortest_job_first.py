from job_class import Job
from scientific_not import sci_notation

class SJF_scheduler:
    def __init__(self):
        self.queue = []
        self.total_completion_time = 0

    def add_job(self, job):
        self.queue.append(job)

    def run(self):
        current_time = 0
        self.queue = sorted(self.queue)
        for job in self.queue:
            current_time += job.remaining_duration
            self.total_completion_time += current_time

    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)

if __name__ == '__main__':
    scheduler = SJF_scheduler()

    # Adding jobs
    numjobs = int(input("Insert number of jobs to process: "))
    filename = r"task_lines.txt"
    with open(filename, "r") as f:
        for i in range(numjobs):
            a = [int(x) for x in f.readline().split(",")]
            scheduler.add_job(Job(a[1], a[0]//1000000, a[2]//1000000))

    # Running the scheduler
    scheduler.run()
    print(f"total_completion_time: {sci_notation(scheduler.total_completion_time)}")
