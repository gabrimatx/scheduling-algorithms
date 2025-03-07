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
        rr_time = 0
        completed_jobs = [False for job in self.queue]
        spjf_index = 0
        completed_count = 0
        time_for_rr = self.hyperLambda
        time_for_spjf = 1 - self.hyperLambda
        for index, job in tqdm(
            enumerate(self.queue),
            total=len(self.queue),
            desc="Enumerating - PRR",
        ):
            job.queue_index = index

        with tqdm(
            total=len(self.queue),
            desc=f"Processing - PRR",
        ) as pbar:
            while len(self.queue) > completed_count:
                remaining_jobs = max(len(self.queue) - completed_count, 1)
                while completed_jobs[spjf_index]:
                    # Move to the next if current greedy selection is already completed
                    spjf_index += 1
                    continue
                pbar.update(1)

                if self.queue[spjf_index] is min_job_heap.get_top():
                    # If greedy selection is also the smallest job
                    rounds_to_complete = (
                        self.queue[spjf_index].remaining_duration
                        - rr_time
                    ) / (time_for_spjf + time_for_rr / remaining_jobs)
                    processing_time = (
                        time_for_rr / remaining_jobs
                    ) * rounds_to_complete

                    # Update how much time will round robin process and objective function
                    rr_time += processing_time
                    self.current_time += rounds_to_complete
                    self.total_completion_time += self.current_time

                    # Mark the job as completed and move it down the heap
                    completed_jobs[spjf_index] = True
                    completed_count += 1
                    self.queue[spjf_index].remaining_duration = float("inf")
                    min_job_heap.heapify(self.queue[spjf_index].heap_index)
                    spjf_index += 1
                else:
                    # Compute how much time is needed to process the smallest job and the greedy selection
                    rounds_to_complete_predicted = (
                        self.queue[spjf_index].remaining_duration
                        - rr_time
                    ) / (time_for_spjf + time_for_rr / remaining_jobs)

                    rounds_to_complete_smallest = (
                        min_job_heap.get_top().remaining_duration
                        - rr_time
                    ) / (time_for_rr / remaining_jobs)

                    if rounds_to_complete_predicted < rounds_to_complete_smallest:
                        # If greedy selection will finish first
                        processing_time = (
                            time_for_rr / remaining_jobs
                        ) * rounds_to_complete_predicted

                        # Update how much time will round robin process and objective function
                        rr_time += processing_time
                        self.current_time += rounds_to_complete_predicted
                        self.total_completion_time += self.current_time

                        # Update objective function and move the completed job down the heap
                        completed_jobs[spjf_index] = True
                        completed_count += 1
                        self.queue[spjf_index].remaining_duration = float("inf")
                        min_job_heap.heapify(self.queue[spjf_index].heap_index)
                        spjf_index += 1
                    else:
                        # If smallest job will finish first

                        # Process greedy selected job for its dedicated slice of time
                        min_job_heap.process_job(
                            self.queue[spjf_index].heap_index,
                            time_for_spjf * rounds_to_complete_smallest,
                        )

                        completed_job = min_job_heap.get_top()

                        processing_time = (
                            time_for_rr / remaining_jobs
                        ) * rounds_to_complete_smallest

                        # Update round robin processing time and objective function
                        rr_time += processing_time
                        self.current_time += rounds_to_complete_smallest
                        self.total_completion_time += self.current_time


                        # Mark completed job and move it down the heap
                        completed_jobs[completed_job.queue_index] = True
                        completed_count += 1
                        self.queue[completed_job.queue_index].remaining_duration = (
                            float("inf")
                        )
                        min_job_heap.heapify(
                            self.queue[completed_job.queue_index].heap_index
                        )


class dPRR_scheduler(Scheduler):
    def __init__(self, lambda_parameter, oracle):
        self.hyperLambda = lambda_parameter
        super().__init__()
        self.oracle = oracle

    def run(self):
        # Similar to static version, but uses class buckets and a dedicated heap

        min_job_heap = HeapWithJobs(self.queue)
        pred_classes = self.oracle.computePredictionClasses(self.queue)
        pred_heap = PredictionHeap(pred_classes)
        job_bucket = JobBucket(self.queue)

        rr_time = 0
        completed_count = 0
        time_for_rr = self.hyperLambda
        time_for_spjf = 1 - self.hyperLambda
        for index, job in tqdm(
            enumerate(self.queue),
            total=len(self.queue),
            desc="Enumerating - dPRR",
        ):
            job.queue_index = index

        with tqdm(total=len(self.queue), desc="Processing - dPRR") as pbar:
            while len(self.queue) > completed_count:
                remaining_jobs = max(len(self.queue) - completed_count, 1)
                pbar.update(1)

                # Obtain greedy selection from smallest predicted bucket based on representative mean
                round_prediction_class = pred_heap.get_top()
                round_predicted_job = job_bucket.get_job(round_prediction_class.id)

                if round_predicted_job is min_job_heap.get_top():
                    # Greedy selection coincides with smallest job
                    rounds_to_complete = (
                        round_predicted_job.remaining_duration
                        - rr_time
                    ) / (time_for_spjf + time_for_rr / remaining_jobs)
                    processing_time = (
                        time_for_rr / remaining_jobs
                    ) * rounds_to_complete

                    rr_time += processing_time
                    self.current_time += rounds_to_complete
                    self.total_completion_time += self.current_time

                    completed_count += 1

                    # Execute job from the bucket and update prediction class and prediction heap consequently
                    job_bucket.exec_job(round_prediction_class.id)
                    self.oracle.updatePrediction(
                        round_predicted_job, pred_heap, round_prediction_class
                    )
                    if job_bucket.is_empty(round_prediction_class.id):
                        pred_heap.empty_prediction_class(round_prediction_class)

                    # Move down the completed job in the heap of real sizes
                    self.queue[round_predicted_job.queue_index].remaining_duration = (
                        float("inf")
                    )
                    min_job_heap.heapify(
                        self.queue[round_predicted_job.queue_index].heap_index
                    )
                else:
                    # Compute rounds needed for greedy selection and smallest job
                    rounds_to_complete_predicted = (
                        round_predicted_job.remaining_duration
                        - rr_time
                    ) / (time_for_spjf + time_for_rr / remaining_jobs)

                    rounds_to_complete_smallest = (
                        min_job_heap.get_top().remaining_duration
                        - rr_time
                    ) / (time_for_rr / remaining_jobs)

                    if rounds_to_complete_predicted < rounds_to_complete_smallest:
                        # Predicted job will finish first
                        processing_time = (
                            time_for_rr / remaining_jobs
                        ) * rounds_to_complete_predicted

                        # Update round robin processed time and objective functions
                        rr_time += processing_time
                        self.current_time += rounds_to_complete_predicted
                        self.total_completion_time += self.current_time

                        completed_count += 1

                        # Execute job and update prediction classes
                        job_bucket.exec_job(round_prediction_class.id)
                        self.oracle.updatePrediction(
                            round_predicted_job, pred_heap, round_prediction_class
                        )
                        if job_bucket.is_empty(round_prediction_class.id):
                            pred_heap.empty_prediction_class(round_prediction_class)

                        # Move completed job down in the heap of job sizes
                        self.queue[round_predicted_job.queue_index].remaining_duration = float("inf")
                        min_job_heap.heapify(
                            self.queue[round_predicted_job.queue_index].heap_index
                        )
                    else:
                        # Smallest job will complete first

                        # Update duration of the greedy selection
                        min_job_heap.process_job(
                            self.queue[round_predicted_job.queue_index].heap_index,
                            time_for_spjf * rounds_to_complete_smallest,
                        )

                        completed_job = min_job_heap.get_top()

                        processing_time = (
                            time_for_rr / remaining_jobs
                        ) * rounds_to_complete_smallest

                        # Update round robin processed time and objective function
                        rr_time += processing_time
                        self.current_time += rounds_to_complete_smallest
                        self.total_completion_time += self.current_time

                        completed_count += 1

                        # TODO: FIX THIS UNEFFICIENT PART

                        # Find prediction class of the completed job and update it
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

                        # Send completed job down the heap of job sizes
                        self.queue[completed_job.queue_index].remaining_duration = (
                            float("inf")
                        )
                        min_job_heap.heapify(
                            self.queue[completed_job.queue_index].heap_index
                        )


class dLambda_scheduler(Scheduler):
    def __init__(self, oracle):
        super().__init__()
        self.oracle = oracle

    def run(self):
        # Similar to dynamic version, but updates the hyperparameter based on the predictions performance

        min_job_heap = HeapWithJobs(self.queue)

        pred_classes = self.oracle.computePredictionClasses(self.queue)
        pred_heap = PredictionHeap(pred_classes)
        job_bucket = JobBucket(self.queue)
        lambda_updater = LambdaUpdaterVersus()
        lmbd_idx = 0
        lmbd_thresh = 2
        
        rr_time = 0
        completed_count = 0
        new_lambda = 0.5

        for index, job in tqdm(
            enumerate(self.queue),
            total=len(self.queue),
            desc="Enumerating - dLambda",
        ):
            job.queue_index = index

        with tqdm(total=len(self.queue), desc="Processing - dLambda") as pbar:
            while len(self.queue) > completed_count:
                remaining_jobs = max(len(self.queue) - completed_count, 1)
                pbar.update(1)

                if lmbd_idx >= lmbd_thresh:
                    # Update hyperparameter at 2,4,8,16... completed jobs
                    new_lambda = lambda_updater.update_lambda(new_lambda)
                    lmbd_thresh = lmbd_thresh ** 2

                lmbd_idx += 1
                time_for_rr = new_lambda
                time_for_spjf = 1 - new_lambda

                round_prediction_class = pred_heap.get_top()
                round_predicted_job = job_bucket.get_job(round_prediction_class.id)

                if round_predicted_job is min_job_heap.get_top():
                    # Greedy selection is smallest job
                    rounds_to_complete = (
                        round_predicted_job.remaining_duration
                        - rr_time
                    ) / (time_for_spjf + time_for_rr / remaining_jobs)
                    processing_time = (
                        time_for_rr / remaining_jobs
                    ) * rounds_to_complete

                    rr_time += processing_time
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
                    # Compute rounds to complete greedy selection and smallest
                    rounds_to_complete_predicted = (
                        round_predicted_job.remaining_duration
                        - rr_time
                    ) / (time_for_spjf + time_for_rr / remaining_jobs)

                    rounds_to_complete_smallest = (
                        min_job_heap.get_top().remaining_duration
                        - rr_time
                    ) / (time_for_rr / remaining_jobs)

                    if rounds_to_complete_predicted < rounds_to_complete_smallest:
                        # Greedy selection will finish first
                        processing_time = (
                            time_for_rr / remaining_jobs
                        ) * rounds_to_complete_predicted

                        rr_time += processing_time
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
                        # Smallest job will finish first
                        min_job_heap.process_job(
                            self.queue[round_predicted_job.queue_index].heap_index,
                            time_for_spjf * rounds_to_complete_smallest,
                        )

                        completed_job = min_job_heap.get_top()                        

                        processing_time = (
                            time_for_rr / remaining_jobs
                        ) * rounds_to_complete_smallest

                        rr_time += processing_time
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
        # Naive scheduler without continuous time relaxation, used for assessing correctness
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
