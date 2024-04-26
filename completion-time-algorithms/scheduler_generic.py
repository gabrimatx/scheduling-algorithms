class Scheduler:
    def __init__(self):
        self.queue = []
        self.total_completion_time = 0
        self.current_time = 0
        self.set_name()

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
        prediction = max(self.oracle.getJobPrediction(job), 0)
        job.prediction = prediction
        return prediction
    
    def set_name(self):
        self.name = self.__class__.__name__
        if self.name in ("PRR_scheduler", "DPRR_scheduler"):
            self.name += " $\lambda$ = " + str(self.hyperLambda)
        elif self.name in ("NCS_scheduler"):
            self.name += " $\epsilon$ = " + str(self.epsilon)