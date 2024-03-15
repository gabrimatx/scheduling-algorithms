import numpy as np
import random
class PerfectOracle:
	def __init__(self):
		pass
	def computePredictions(self, job):
		pass
	def getJobPrediction(self, job):
		return job[2]

class JobMeanOracle:
	def __init__(self):
		self.jobMeans = {}
		self.jobOccurrences = {}
		self.totalJobOccurrences = 0
		self.totalMean = 0

	def getJobPrediction(self, Job):
		return self.jobMeans.get(int(Job.id), self.totalMean)

	def computePredictions(self, JobSet):
		for Job in JobSet:
			Job[1] = int(Job[1])
			if Job[1] in self.jobMeans:
				self.jobMeans[Job[1]] = ((self.jobMeans[Job[1]] * self.jobOccurrences[Job[1]]) + Job[2]) / (self.jobOccurrences[Job[1]] + 1)
				self.jobOccurrences[Job[1]] += 1
			else:
				self.jobMeans[Job[1]] = Job[2]
				self.jobOccurrences[Job[1]] = 1

			self.totalMean = ((self.totalMean * self.totalJobOccurrences) + Job[2]) / (self.totalJobOccurrences + 1)
			self.totalJobOccurrences += 1

class GaussianPerturbationOracle:
	def __init__(self, mean, std_dev):
		self.mean = mean
		self.std_dev = std_dev

	def computePredictions(self, JobSet):
		pass

	def getJobPrediction(self, Job):
		return Job.real_duration + np.random.normal(self.mean, self.std_dev, 1)[0]



