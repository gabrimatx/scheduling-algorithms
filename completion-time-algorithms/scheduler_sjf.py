from scheduler_generic import Scheduler
from tqdm import tqdm
from job_class import JobBucket
from my_heap import PredictionHeap
from copy import deepcopy
import numpy as np
import random


class SJF_scheduler(Scheduler):
    def run(self):
        self.queue = sorted(self.queue)
        for job in tqdm(self.queue, total=len(self.queue), desc="Processing - SJF"):
            self.current_time += job.remaining_duration
            self.total_completion_time += self.current_time


class SPJF_scheduler(Scheduler):
    def __init__(self, oracle):
        super().__init__()
        self.oracle = oracle

    def run(self):
        self.queue.sort(key=lambda j: self.sort_jobs(j))
        for job in tqdm(
            self.queue, total=len(self.queue), desc="Processing - SPJF", disable=True
        ):
            self.current_time += job.remaining_duration
            self.total_completion_time += self.current_time


class dSPJF_scheduler(Scheduler):
    def __init__(self, oracle):
        super().__init__()
        self.oracle = oracle

    def run(self):
        pred_classes = self.oracle.computePredictionClasses(self.queue)
        pred_heap = PredictionHeap(pred_classes)
        job_bucket = JobBucket(self.queue)
        for i in tqdm(range(len(self.queue)), desc="Running - dSPJF"):
            # Obtain job from the bucket and execute it
            prediction_class = pred_heap.get_top()
            completed_job = job_bucket.exec_job(prediction_class.id)

            # Update the predictions and drop the job class down the heap if empty
            self.oracle.updatePrediction(completed_job, pred_heap, prediction_class)
            if job_bucket.is_empty(prediction_class.id):
                pred_heap.empty_prediction_class(prediction_class)

            # Update the objective
            self.current_time += completed_job.remaining_duration
            self.total_completion_time += self.current_time


class Lottery_scheduler(Scheduler):
    def __init__(self, oracle):
        super().__init__()
        self.oracle = oracle

    def run(self):
        true_queue = deepcopy(self.queue)
        random.seed(22)
        results = []
        for i in tqdm(range(10), desc="Processing - Lottery"):
            self.current_time = 0
            self.total_completion_time = 0
            self.queue = deepcopy(true_queue)
            self.random_run()
            results.append(self.total_completion_time)
        self.total_completion_time = np.mean(results)
        self.std = results

    def random_run(self):
        job_bucket = JobBucket(self.queue)
        next_id = self.oracle.pick_next(job_bucket.get_classes())
        for i in range(len(self.queue)):
            # We select a job bucket and run its contents until we finish all the jobs in the class
            if job_bucket.is_empty(next_id):
                # Run lottery
                next_id = self.oracle.pick_next(job_bucket.get_classes())

            # Update predictions
            completed_job = job_bucket.exec_job(next_id)
            self.oracle.updatePrediction(completed_job)
            self.current_time += completed_job.remaining_duration
            self.total_completion_time += self.current_time
