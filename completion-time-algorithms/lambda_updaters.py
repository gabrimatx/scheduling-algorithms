from scheduler_rr import RR_scheduler
from scheduler_sjf import SPJF_scheduler
from oracles import PredictedOracle
from copy import deepcopy

class LambdaUpdaterVersus:
    def __init__(self) -> None:
        self.jobset = []
    
    def update_error(self, job):
        self.jobset.append(job)
    
    def update_lambda(self, lmbda):
        if not self.jobset:
            return 0.5
        my_rr, my_spjf = RR_scheduler(), SPJF_scheduler(PredictedOracle()) # TODO: Why not dSPJF?
        for sched in [my_spjf, my_rr]:
            sched.add_job_set(deepcopy(self.jobset))
            sched.run()
        rr_perf, spjf_perf = my_rr.total_completion_time, my_spjf.total_completion_time
        if rr_perf == 0 or spjf_perf == 0:
            return 0.5
        if rr_perf < spjf_perf:
            increase = (spjf_perf - rr_perf) / spjf_perf
            lmbda += (1 - lmbda) * increase
        else:
            decrease = (rr_perf - spjf_perf) / rr_perf
            lmbda -= lmbda * decrease

        return lmbda

class LambdaUpdaterNaive:
    def __init__(self) -> None:
        self.max_job_size = 0
        self.executed_jobs = 0
        self.bad_ordered_jobs = 0
    
    def update_error(self, job):
        if job.real_duration < self.max_job_size:
            self.bad_ordered_jobs += 1
        else:
            self.max_job_size = job.real_duration        
        self.executed_jobs += 1

    def update_lambda(self, lmbda):
        if self.executed_jobs > 0:
            return max(self.bad_ordered_jobs / self.executed_jobs, 0.01)
        else:
            return 0.5