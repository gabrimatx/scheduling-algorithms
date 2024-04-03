from job_class import Job, JobBucket, PredictionClass
from my_heap import PredictionHeap
from scientific_not import sci_notation
from tqdm import tqdm
from oracles import DynamicJobMeanOracle
import random

class DSPJF_scheduler:
    def __init__(self, oracle):
        self.queue = []
        self.total_completion_time = 0
        self.total_error = 0
        self.oracle = oracle

    def add_job(self, job):
        self.queue.append(job)

    def add_job_set(self, jobset):
        for job in jobset:
            self.add_job(job)

    def run(self):
        current_time = 0
        pred_classes = self.oracle.computePredictionClasses(self.queue)
        pred_heap = PredictionHeap(pred_classes)
        job_bucket = JobBucket(self.queue)
        for i in tqdm(range(len(self.queue)), desc="Running dspjf..."):
            prediction_class = pred_heap.get_top()
            completed_job = job_bucket.exec_job(prediction_class.id)
            self.oracle.updatePrediction(completed_job, pred_heap, prediction_class)
            if job_bucket.is_empty(prediction_class.id):
                pred_heap.empty_prediction_class(prediction_class)
            current_time += completed_job.remaining_duration
            self.total_completion_time += current_time

    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)
