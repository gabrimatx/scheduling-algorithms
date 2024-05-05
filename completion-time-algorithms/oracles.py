import numpy as np
from job_class import Job, PredictionClass
import numpy as np
from collections import defaultdict
from random import choice
from itertools import chain
class PerfectOracle:
    def __init__(self):
        pass

    def computePredictions(self, job: Job) -> None:
        pass

    def getJobPrediction(self, job: Job) -> None:
        return job.real_duration
    
    def updatePrediction_NH(self, job):
        pass

class PredictedOracle:
    def __init__(self) -> None:
        pass
    def computePredictions(self, job: Job):
        pass
    def getJobPrediction(self, job: Job) -> None:
        return job.prediction

class JobMeanOracle:
    def __init__(self):
        self.jobTotals = {}
        self.jobOccurrences = {}
        self.totalJobOccurrences = 0
        self.totalMean = 0

    def reset_data(self):
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
    
    def updatePrediction_NH(self, job):
        if job.id in self.jobTotals:
            self.jobTotals[job.id] += job.real_duration
            self.jobOccurrences[job.id] += 1
        else:
            self.jobTotals[job.id] = job.real_duration
            self.jobOccurrences[job.id] = 1
        self.totalMean += job.real_duration
        self.totalJobOccurrences += 1

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

class LotteryOracle:
    def __init__(self, rounds) -> None:
        self.completed_jobs = defaultdict(list)
        self.rounds = rounds
        self.jobTotal = 0
        self.jobNum = 0

    def updatePrediction(self, job):
        self.completed_jobs[job.id].append(job)
        self.jobTotal += job.real_duration
        self.jobNum += 1

    def computePredictions(self, JobSet: list) -> None:
        for job in JobSet:
            self.completed_jobs[job.id].append(job)
        self.jobTotal = sum(x.real_duration for x in chain.from_iterable(self.completed_jobs.values()))
        self.jobNum = sum(len(x) for x in self.completed_jobs.values())

    def pick_next(self, classes):
        # Returns the selected class
        if self.jobNum == 0:
            return choice(list(classes))
        scores = defaultdict(int)
        for _ in range(self.rounds):
            candidates = []
            for job_class in classes:
                if self.completed_jobs[job_class]:
                    candidates.append(choice(self.completed_jobs[job_class]))
                else:
                    candidates.append(Job(job_class, 0, self.jobTotal / self.jobNum))
            smallest_job = min(candidates, key = lambda x: x.real_duration)
            scores[smallest_job.id] += 1
        return max(scores.keys(), key = lambda class_id: scores[class_id])

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
