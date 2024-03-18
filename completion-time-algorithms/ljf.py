from oracles import *
from job_class import Job
from scientific_not import sci_notation
from tqdm import tqdm

class LJF_scheduler:
    def __init__(self):
        self.queue = []
        self.total_completion_time = 0
        self.total_error = 0

    def add_job(self, job):
        self.queue.append(job)

    def add_job_set(self, jobset):
        for job in jobset:
            self.add_job(job)

    def sort_and_add_error(self, job):
        prediction = self.oracle.getJobPrediction(job)
        self.total_error += abs(job.real_duration - prediction)
        return prediction

    def run(self):
        current_time = 0
        self.queue.sort(reverse = True)
        with tqdm(total=len(self.queue), desc = "Processing (lpjf)...") as pbar:
            while self.queue:
                job = self.queue[0]
                self.queue = self.queue[1:]
                current_time += job.remaining_duration
                self.total_completion_time += current_time
                pbar.update(1)

    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)

if __name__ == '__main__':
    scheduler = LJF_scheduler()
    numjobs = int(input("Insert number of jobs to process: "))
    filename = r"task_lines.txt"
    with open(filename, "r") as f:
        for i in range(numjobs):
            a = [int(x) for x in f.readline().split(",")]
            scheduler.add_job(Job(a[1], a[0]//1000000, a[2]//1000000))
    # Running the scheduler
    scheduler.run()
    print(f"total_completion_time: {sci_notation(scheduler.total_completion_time)} competitive_ratio: {(2 * scheduler.total_error / numjobs) + 1}")