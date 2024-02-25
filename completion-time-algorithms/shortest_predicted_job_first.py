from oracles import *
from job_class import Job

class SPJF_scheduler:
    def __init__(self):
        self.queue = []
        self.total_completion_time = 0
        self.total_error = 0

    def add_job(self, job):
        self.queue.append(job)

    def sort_and_add_error(self, job):
        prediction = oracle.getJobPrediction(job)
        self.total_error += abs(job.predicted_duration - prediction)
        return prediction

    def run(self):
        current_time = 0
        oracle.computePredictions(self.queue)
        self.queue.sort(key = lambda j: self.sort_and_add_error(j))
        while self.queue:
            job = self.queue[0]
            self.queue = self.queue[1:]
            current_time += job.remaining_duration
            self.total_completion_time += current_time

    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)

if __name__ == '__main__':
	scheduler = SPJF_scheduler()
	numjobs = int(input("Insert number of jobs to process: "))
	oracle = JobMeanOracle()
	filename = r"task_lines.txt"
	with open(filename, "r") as f:
	    for i in range(numjobs):
	        a = [int(x) for x in f.readline().split(",")]
	        scheduler.add_job(Job(a[1], a[0]//1000, a[2]//1000))
	# Running the scheduler
	scheduler.run()
	print(f"total_completion_time: {scheduler.total_completion_time} competitive_ratio: {(2 * scheduler.total_error / numjobs) + 1}")



