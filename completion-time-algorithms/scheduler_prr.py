from my_heap import HeapWithJobs
from tqdm import tqdm
from copy import copy
from scheduler_generic import Scheduler
from job_class import JobBucket
from my_heap import PredictionHeap
from lambda_updaters import LambdaUpdaterVersus

class PRR_scheduler(Scheduler):
    def __init__(self, lambda_parameter, oracle):
        self.hyperLambda = lambda_parameter
        super().__init__()
        self.oracle = oracle

    def run(self):
        min_job_heap = HeapWithJobs(self.queue)
        self.queue.sort(key=lambda j: self.sort_jobs(j))
        round_robin_processed_time = 0
        completed_jobs = [False for job in self.queue]
        spjf_index = 0
        completed_count = 0
        time_for_rr = self.hyperLambda
        time_for_spjf = 1 - self.hyperLambda
        for index, job in tqdm(
            enumerate(self.queue),
            total=len(self.queue),
            desc="Enumerating jobs (prr)...",
        ):
            job.queue_index = index

        with tqdm(
            total=len(self.queue),
            desc=f"Processing (prr lambda = {self.hyperLambda})...",
        ) as pbar:
            while len(self.queue) > completed_count:
                remaining_jobs = max(len(self.queue) - completed_count, 1)
                while completed_jobs[spjf_index]:
                    spjf_index += 1
                    continue
                pbar.update(1)
                if self.queue[spjf_index] is min_job_heap.get_top():
                    rounds_to_complete = (
                        self.queue[spjf_index].remaining_duration
                        - round_robin_processed_time
                    ) / (time_for_spjf + time_for_rr / remaining_jobs)
                    processing_time = (
                        time_for_rr / remaining_jobs
                    ) * rounds_to_complete
                    round_robin_processed_time += processing_time
                    self.current_time += rounds_to_complete
                    self.total_completion_time += self.current_time

                    completed_jobs[spjf_index] = True
                    completed_count += 1
                    self.queue[spjf_index].remaining_duration = float("inf")
                    min_job_heap.heapify(self.queue[spjf_index].heap_index)
                    spjf_index += 1

                else:
                    rounds_to_complete_predicted = (
                        self.queue[spjf_index].remaining_duration
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
                        self.current_time += rounds_to_complete_predicted
                        self.total_completion_time += self.current_time

                        completed_jobs[spjf_index] = True
                        completed_count += 1
                        self.queue[spjf_index].remaining_duration = float("inf")
                        min_job_heap.heapify(self.queue[spjf_index].heap_index)
                        spjf_index += 1
                    else:
                        min_job_heap.process_job(
                            self.queue[spjf_index].heap_index,
                            time_for_spjf * rounds_to_complete_smallest,
                        )

                        completed_job = min_job_heap.get_top()

                        processing_time = (
                            time_for_rr / remaining_jobs
                        ) * rounds_to_complete_smallest

                        round_robin_processed_time += processing_time
                        self.current_time += rounds_to_complete_smallest
                        self.total_completion_time += self.current_time

                        completed_jobs[completed_job.queue_index] = True
                        completed_count += 1
                        self.queue[completed_job.queue_index].remaining_duration = (
                            float("inf")
                        )
                        min_job_heap.heapify(
                            self.queue[completed_job.queue_index].heap_index
                        )


class DPRR_scheduler(Scheduler):
    def __init__(self, lambda_parameter, oracle):
        self.hyperLambda = lambda_parameter
        super().__init__()
        self.oracle = oracle

    def run(self):

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
                    self.current_time += rounds_to_complete
                    self.total_completion_time += self.current_time

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
                        self.current_time += rounds_to_complete_predicted
                        self.total_completion_time += self.current_time

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
                        self.current_time += rounds_to_complete_smallest
                        self.total_completion_time += self.current_time

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


class DPRR_dlambda_scheduler(Scheduler):
    def __init__(self, oracle):
        super().__init__()
        self.oracle = oracle

    def run(self):

        min_job_heap = HeapWithJobs(self.queue)

        pred_classes = self.oracle.computePredictionClasses(self.queue)
        pred_heap = PredictionHeap(pred_classes)
        job_bucket = JobBucket(self.queue)
        lambda_updater = LambdaUpdaterVersus()
        lmbd_idx = 0
        lmbd_thresh = 2
        
        round_robin_processed_time = 0
        completed_count = 0
        new_lambda = 0.5

        for index, job in tqdm(
            enumerate(self.queue),
            total=len(self.queue),
            desc="Enumerating jobs (dynamic prr lambda)...",
        ):
            job.queue_index = index

        with tqdm(total=len(self.queue), desc="Processing (dynamic prr lambda)...") as pbar:
            while len(self.queue) > completed_count:
                remaining_jobs = max(len(self.queue) - completed_count, 1)
                pbar.update(1)

                if lmbd_idx >= lmbd_thresh:
                    new_lambda = lambda_updater.update_lambda(new_lambda)
                    lmbd_thresh = lmbd_thresh ** 2
                    print(new_lambda)

                lmbd_idx += 1
                time_for_rr = new_lambda
                time_for_spjf = 1 - new_lambda

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
                    self.current_time += rounds_to_complete
                    self.total_completion_time += self.current_time

                    completed_count += 1

                    completed_job = job_bucket.exec_job(round_prediction_class.id)
                    completed_job.prediction = round_prediction_class.prediction
                    lambda_updater.update_error(copy(completed_job))

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
                        self.current_time += rounds_to_complete_predicted
                        self.total_completion_time += self.current_time

                        completed_count += 1

                        completed_job = job_bucket.exec_job(round_prediction_class.id)
                        completed_job.prediction = round_prediction_class.prediction
                        lambda_updater.update_error(copy(completed_job))

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
                        self.current_time += rounds_to_complete_smallest
                        self.total_completion_time += self.current_time

                        completed_count += 1

                        # TODO: FIX THIS UNEFFICIENT PART
                        job_bucket.pop_job(completed_job)
                        pred_class = 0
                        for c in pred_heap.container:
                            if c.id == completed_job.id:
                                pred_class = c
                        if not pred_class:
                            raise Exception

                        completed_job.prediction = pred_class.prediction
                        lambda_updater.update_error(copy(completed_job))


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



class PRR_naive_scheduler(Scheduler):
    def __init__(self, lambda_parameter, oracle):
        super().__init__()
        self.round_time = 1
        self.hyperLambda = lambda_parameter
        self.quantum = 0.001
        self.oracle = oracle

    def run(self):
        self.queue.sort(key=lambda j: self.sort_jobs(j))
        while self.queue:
            time_for_rr = self.round_time * self.hyperLambda
            time_for_spjf = self.round_time * (1 - self.hyperLambda)
            rr_index = 0

            while time_for_spjf and self.queue:
                if self.queue[0].remaining_duration <= time_for_spjf:
                    self.current_time += self.queue[0].remaining_duration
                    time_for_spjf -= self.queue[0].remaining_duration
                    self.queue = self.queue[1:]
                    self.total_completion_time += self.current_time
                else:
                    self.current_time += time_for_spjf
                    self.queue[0].remaining_duration -= time_for_spjf
                    time_for_spjf = 0

            if not self.queue:
                break

            round_quantum = self.quantum

            while time_for_rr and self.queue:
                rr_index = rr_index % len(self.queue)
                if self.queue[rr_index].remaining_duration <= min(
                    time_for_rr, round_quantum
                ):
                    self.current_time += self.queue[rr_index].remaining_duration
                    time_for_rr -= self.queue[rr_index].remaining_duration
                    self.queue.pop(rr_index)
                    self.total_completion_time += self.current_time
                else:
                    self.current_time += min(time_for_rr, round_quantum)
                    time_for_rr -= min(time_for_rr, round_quantum)
                    self.queue[rr_index].remaining_duration -= min(
                        time_for_rr, round_quantum
                    )
                    rr_index += 1
