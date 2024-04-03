from oracles import DynamicJobMeanOracle
from job_class import Job, PredictionClass, JobBucket
from scientific_not import sci_notation
from my_heap import HeapWithJobs, PredictionHeap
from tqdm import tqdm
import time


class DPRR_scheduler:
    def __init__(self, lambda_parameter, oracle):
        self.queue = []
        self.total_completion_time = 0
        self.hyperLambda = lambda_parameter
        self.total_error = 0
        self.oracle = oracle

    def add_job_set(self, jobset):
        for job in jobset:
            self.add_job(job)

    def add_job(self, job):
        self.queue.append(job)

    def run(self):
        current_time = 0

        min_job_heap = HeapWithJobs(self.queue)

        pred_classes = self.oracle.computePredictionClasses(self.queue)
        pred_heap = PredictionHeap(pred_classes)
        job_bucket = JobBucket(self.queue)

        round_robin_processed_time = 0
        completed_count = 0
        time_for_rr = self.hyperLambda
        time_for_spjf = 1 - self.hyperLambda
        for index, job in tqdm(
            enumerate(self.queue),
            total=len(self.queue),
            desc="Enumerating jobs (dynamic prr)...",
        ):
            job.queue_index = index

        with tqdm(total=len(self.queue), desc="Processing (dynamic prr)...") as pbar:
            while len(self.queue) > completed_count:
                remaining_jobs = max(len(self.queue) - completed_count, 1)
                pbar.update(1)

                round_prediction_class = pred_heap.get_top()
                round_predicted_job = job_bucket.get_job(round_prediction_class.id)

                if round_predicted_job is min_job_heap.get_top():
                    rounds_to_complete = (
                        round_predicted_job.remaining_duration
                        - round_robin_processed_time
                    ) / (time_for_spjf + time_for_rr / remaining_jobs)
                    processing_time = (
                        time_for_rr / remaining_jobs
                    ) * rounds_to_complete

                    round_robin_processed_time += processing_time
                    current_time += rounds_to_complete
                    self.total_completion_time += current_time

                    completed_count += 1

                    job_bucket.exec_job(round_prediction_class.id)
                    self.oracle.updatePrediction(
                        round_predicted_job, pred_heap, round_prediction_class
                    )
                    if job_bucket.is_empty(round_prediction_class.id):
                        pred_heap.empty_prediction_class(round_prediction_class)

                    self.queue[round_predicted_job.queue_index].remaining_duration = (
                        float("inf")
                    )
                    min_job_heap.heapify(
                        self.queue[round_predicted_job.queue_index].heap_index
                    )
                else:
                    rounds_to_complete_predicted = (
                        round_predicted_job.remaining_duration
                        - round_robin_processed_time
                    ) / (time_for_spjf + time_for_rr / remaining_jobs)

                    rounds_to_complete_smallest = (
                        min_job_heap.get_top().remaining_duration
                        - round_robin_processed_time
                    ) / (time_for_rr / remaining_jobs)

                    if rounds_to_complete_predicted < rounds_to_complete_smallest:
                        processing_time = (
                            time_for_rr / remaining_jobs
                        ) * rounds_to_complete_predicted

                        round_robin_processed_time += processing_time
                        current_time += rounds_to_complete_predicted
                        self.total_completion_time += current_time

                        completed_count += 1

                        job_bucket.exec_job(round_prediction_class.id)
                        self.oracle.updatePrediction(
                            round_predicted_job, pred_heap, round_prediction_class
                        )
                        if job_bucket.is_empty(round_prediction_class.id):
                            pred_heap.empty_prediction_class(round_prediction_class)

                        self.queue[
                            round_predicted_job.queue_index
                        ].remaining_duration = float("inf")
                        min_job_heap.heapify(
                            self.queue[round_predicted_job.queue_index].heap_index
                        )
                    else:
                        min_job_heap.process_job(
                            self.queue[round_predicted_job.queue_index].heap_index,
                            time_for_spjf * rounds_to_complete_smallest,
                        )

                        completed_job = min_job_heap.get_top()

                        processing_time = (
                            time_for_rr / remaining_jobs
                        ) * rounds_to_complete_smallest

                        round_robin_processed_time += processing_time
                        current_time += rounds_to_complete_smallest
                        self.total_completion_time += current_time

                        completed_count += 1

                        # TODO: FIX THIS UNEFFICIENT PART
                        job_bucket.pop_job(completed_job)
                        pred_class = 0
                        for c in pred_heap.container:
                            if c.id == completed_job.id:
                                pred_class = c
                        if not pred_class:
                            raise Exception
                        self.oracle.updatePrediction(
                            completed_job, pred_heap, pred_class
                        )
                        if job_bucket.is_empty(pred_class.id):
                            pred_heap.empty_prediction_class(pred_class)

                        self.queue[completed_job.queue_index].remaining_duration = (
                            float("inf")
                        )
                        min_job_heap.heapify(
                            self.queue[completed_job.queue_index].heap_index
                        )

    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)
