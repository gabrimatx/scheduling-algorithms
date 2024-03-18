import numpy as np
import random
from job_class import Job
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
		return self.jobTotals.get(job.id, self.totalMean) / self.jobOccurrences.get(job.id, self.totalJobOccurrences)

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

class GaussianPerturbationOracle:
	def __init__(self, mean, std_dev):
		self.mean = mean
		self.std_dev = std_dev

	def computePredictions(self, JobSet):
		pass

	def getJobPrediction(self, Job):
		return Job.real_duration + np.random.normal(self.mean, self.std_dev, 1)[0]
	




