import numpy as np
from job_class import Job, PredictionClass
import numpy as np

class PerfectOracle:
    def __init__(self):
        pass

    def computePredictions(self, job: Job) -> None:
        pass

    def getJobPrediction(self, job: Job) -> None:
        return job.real_duration

class JobMeanOracle:
    def __init__(self):
        self.jobTotals = {}
        self.jobOccurrences = {}
        self.totalJobOccurrences = 0
        self.totalMean = 0

    def getJobPrediction(self, job: Job) -> float:
        return self.jobTotals.get(job.id, self.totalMean) / max(
            self.jobOccurrences.get(job.id, self.totalJobOccurrences), 1
        )

    def computePredictions(self, JobSet: list) -> None:
        for job in JobSet:
            if job.id in self.jobTotals:
                self.jobTotals[job.id] += job.real_duration
                self.jobOccurrences[job.id] += 1
            else:
                self.jobTotals[job.id] = job.real_duration
                self.jobOccurrences[job.id] = 1

            self.totalMean += job.real_duration
            self.totalJobOccurrences += 1

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

    def update_lambda(self):
        if self.executed_jobs > 0:
            return max(self.bad_ordered_jobs / self.executed_jobs, 0.01)
        else:
            return 0.5
        
class DynamicJobMeanOracle(JobMeanOracle):
    def __init__(self):
        super().__init__()
    
    def computePredictionClasses(self, JobSet) -> list:
        d = {}
        for job in JobSet:
            if job.id in d:
                d[job.id].size_j += 1
            else:
                d[job.id] = PredictionClass(job.id, 1, self.getJobPrediction(job))
        return list(d.values())
    
    def updatePrediction(self, job, P_heap, P_class):
        if job.id in self.jobTotals:
            self.jobTotals[job.id] += job.real_duration
            self.jobOccurrences[job.id] += 1
        else:
            self.jobTotals[job.id] = job.real_duration
            self.jobOccurrences[job.id] = 1
        P_heap.update_prediction(P_class, self.jobTotals[job.id] / self.jobOccurrences[job.id])
        self.totalMean += job.real_duration
        self.totalJobOccurrences += 1

class JobMedianOracle:
    def __init__(self):
        self.jobMedians = {}
        self.totalMedian = 0

    def getJobPrediction(self, job: Job) -> float:
        return self.jobMedians.get(job.id, self.totalMedian)
    
    def median(self, lst):
        sorted_lst = sorted(lst)
        n = len(sorted_lst)
        if n % 2 == 0:
            mid = n // 2
            return (sorted_lst[mid - 1] + sorted_lst[mid]) / 2
        else:
            return sorted_lst[n // 2]

    def computePredictions(self, JobSet: list) -> None:
        occurrences_per_id = {}
        total_occurrences = []
        for job in JobSet:
            if job.id in occurrences_per_id:
                occurrences_per_id[job.id].append(job.real_duration)
            else:
                occurrences_per_id[job.id] = [job.real_duration]

            total_occurrences.append(job.real_duration)
            
        for k,v in occurrences_per_id.items():
            self.jobMedians[k] = self.median(v)
        self.totalMedian = self.median(total_occurrences)
                
class GaussianPerturbationOracle:
    def __init__(self, mean, std_dev):
        self.mean = mean
        self.std_dev = std_dev

    def computePredictions(self, JobSet):
        pass

    def getJobPrediction(self, Job):
        return Job.real_duration + np.random.normal(self.mean, self.std_dev, 1)[0]

if __name__ == "__main__":
    a = DynamicJobMeanOracle()
    h = [Job(i % 2, 0, i + 200) for i in range(20)]
    a.computePredictions(h)
    print(a.computePredictionClasses())
