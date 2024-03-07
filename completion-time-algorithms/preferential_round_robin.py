from oracles import *
from job_class import Job
from scientific_not import sci_notation

class PRR_scheduler:
    def __init__(self, lamb):
        self.queue = []
        self.total_completion_time = 0
        self.round_time = 10000000
        self.quantum = 100000
        self.hyperLambda = lamb
        self.total_error = 0

    def add_job(self, job):
        self.queue.append(job)

    def sort_and_add_error(self, job):
        prediction = oracle.getJobPrediction(job)
        self.total_error += abs(job.real_duration - prediction)
        return prediction

    def run(self):
        current_time = 0
        oracle.computePredictions(self.queue[:(len(self.queue) // 100 * 20)])
        self.queue = self.queue[(len(self.queue) // 100 * 20):]
        self.queue.sort(key = lambda j: self.sort_and_add_error(j))
        while self.queue:
            time_for_rr = self.round_time * self.hyperLambda
            time_for_spjf = self.round_time * (1 - self.hyperLambda)
            rr_index = 0

            # Run spjf for round_time * lambda
            while time_for_spjf and self.queue:
                if self.queue[0].remaining_duration <= time_for_spjf:
                    current_time += self.queue[0].remaining_duration
                    time_for_spjf -= self.queue[0].remaining_duration
                    self.queue = self.queue[1:]
                    self.total_completion_time += current_time
                else:
                    current_time += time_for_spjf
                    self.queue[0].remaining_duration -= time_for_spjf
                    time_for_spjf = 0

            while time_for_rr and self.queue:
                rr_index = rr_index % len(self.queue)
                if self.queue[rr_index].remaining_duration <= min(time_for_rr, self.quantum):
                    current_time += self.queue[rr_index].remaining_duration
                    time_for_rr -= self.queue[rr_index].remaining_duration
                    self.queue.pop(rr_index)
                    self.total_completion_time += current_time
                else:
                    current_time += min(time_for_rr, self.quantum)
                    time_for_rr -= min(time_for_rr, self.quantum)
                    self.queue[rr_index].remaining_duration -= min(time_for_rr, self.quantum)
                    rr_index += 1


    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)


if __name__ == '__main__':
    l = float(input("Insert lambda hyperparameter: "))
    scheduler = PRR_scheduler(l)
    numjobs = int(input("Insert number of jobs to process: "))
    oracle = JobMeanOracle()
    filename = r"task_lines.txt"
    with open(filename, "r") as f:
        for i in range(numjobs):
            a = [int(x) for x in f.readline().split(",")]
            scheduler.add_job(Job(a[1], a[0]//1000, a[2]//1000))
    # Running the scheduler
    scheduler.run()
    print(f"total_completion_time: {sci_notation(scheduler.total_completion_time)} competitive_ratio: {min(((2 * scheduler.total_error / numjobs) + 1) / (1 - l), 2.0 / l)}")