import numpy as np

class JobMeanOracle:
	def __init__(self):
		self.jobMeans = {}
		self.jobOccurrences = {}
		self.totalJobOccurrences = 0
		self.totalMean = 0

	def getJobPrediction(self, Job):
		return self.jobMeans.get(Job.id, self.totalMean)

	def computePredictions(self, JobSet):
		for Job in JobSet:
			if Job.id in self.jobMeans:
				self.jobMeans[Job.id] = ((self.jobMeans[Job.id] * self.jobOccurrences[Job.id]) + Job.real_duration) / (self.jobOccurrences[Job.id] + 1)
				self.jobOccurrences[Job.id] += 1
			else:
				self.jobMeans[Job.id] = Job.real_duration
				self.jobOccurrences[Job.id] = 1

			self.totalMean = ((self.totalMean * self.totalJobOccurrences) + Job.real_duration) / (self.totalJobOccurrences + 1)
			self.totalJobOccurrences += 1

class GaussianPerturbationOracle:
	def __init__(self, mean, std_dev):
		self.mean = mean
		self.std_dev = std_dev

	def computePredictions(self, JobSet):
		pass

	def getJobPrediction(self, Job):
		return Job.real_duration + np.random.normal(self.mean, self.std_dev, 1)[0]



