class Scheduler:
    def __init__(self):
        self.queue = []
        self.total_completion_time = 0

    def add_job_set(self, jobset):
        for job in jobset:
            self.add_job(job)

    def add_job(self, job):
        self.queue.append(job)
    
    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)

    def sort_jobs(self, job):
        prediction = self.oracle.getJobPrediction(job)
        return prediction