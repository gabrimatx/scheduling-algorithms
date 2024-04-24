from scheduler_generic import Scheduler
from tqdm import tqdm
from job_class import JobBucket
from my_heap import PredictionHeap


class SJF_scheduler(Scheduler):
    def run(self):
        self.queue = sorted(self.queue)
        for job in tqdm(self.queue, total=len(self.queue), desc="Processing (sjf)..."):
            self.current_time += job.remaining_duration
            self.total_completion_time += self.current_time


class SPJF_scheduler(Scheduler):
    def __init__(self, oracle):
        super().__init__()
        self.oracle = oracle

    def run(self):
        self.queue.sort(key=lambda j: self.sort_jobs(j))
        for i in tqdm(range(len(self.queue)), "Processing (spjf)..."):
            job = self.queue[i]
            self.current_time += job.remaining_duration
            self.total_completion_time += self.current_time


class DSPJF_scheduler(Scheduler):
    def __init__(self, oracle):
        super().__init__()
        self.oracle = oracle

    def run(self):
        pred_classes = self.oracle.computePredictionClasses(self.queue)
        pred_heap = PredictionHeap(pred_classes)
        job_bucket = JobBucket(self.queue)
        for i in tqdm(range(len(self.queue)), desc="Running dspjf..."):
            prediction_class = pred_heap.get_top()
            completed_job = job_bucket.exec_job(prediction_class.id)
            self.oracle.updatePrediction(completed_job, pred_heap, prediction_class)
            if job_bucket.is_empty(prediction_class.id):
                pred_heap.empty_prediction_class(prediction_class)
            self.current_time += completed_job.remaining_duration
            self.total_completion_time += self.current_time
