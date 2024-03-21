from oracles import *
from job_class import Job
from scientific_not import sci_notation
from my_heap import heap
from tqdm import tqdm
import time


class PRR_scheduler:
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

    def sort_and_add_error(self, job):
        prediction = self.oracle.getJobPrediction(job)
        self.total_error += abs(job.real_duration - prediction)
        return prediction

    def run(self):
        current_time = 0
        min_job_heap = heap(self.queue)
        self.queue.sort(key=lambda j: self.sort_and_add_error(j))
        round_robin_processed_time = 0
        time_for_rr = self.hyperLambda
        time_for_spjf = 1 - self.hyperLambda
        with tqdm(total=len(self.queue), desc = "Processing (prr)...") as pbar:
            while len(self.queue) > 1:
                pbar.update(1)
                if self.queue[0] is min_job_heap.get_top():
                    rounds_to_complete = (
                        self.queue[0].remaining_duration - round_robin_processed_time
                    ) / (time_for_spjf + time_for_rr / len(self.queue))
                    completed_job = self.queue.pop(0)
                    min_job_heap.pop_head()
                    processing_time = (time_for_rr / len(self.queue)) * rounds_to_complete

                    round_robin_processed_time += processing_time
                    current_time += rounds_to_complete
                    self.total_completion_time += current_time
                else:
                    rounds_to_complete_predicted = (
                        self.queue[0].remaining_duration - round_robin_processed_time
                    ) / (time_for_spjf + time_for_rr / len(self.queue))

                    rounds_to_complete_smallest = (
                        min_job_heap.get_top().remaining_duration
                        - round_robin_processed_time
                    ) / (time_for_rr / len(self.queue))

                    if rounds_to_complete_predicted < rounds_to_complete_smallest:
                        processing_time = (
                            time_for_rr / len(self.queue)
                        ) * rounds_to_complete_predicted

                        completed_job = self.queue.pop(0)
                        min_job_heap.pop_at_index(completed_job.heap_index)

                        round_robin_processed_time += processing_time
                        current_time += rounds_to_complete_predicted
                        self.total_completion_time += current_time
                    else:
                        min_job_heap.process_job(
                            self.queue[0].heap_index,
                            time_for_spjf * rounds_to_complete_smallest,
                        )

                        completed_job = min_job_heap.pop_head()
                        for index, job in enumerate(self.queue):
                            if job is completed_job:
                                index_to_pop = index

                        processing_time = (
                            time_for_rr / len(self.queue)
                        ) * rounds_to_complete_smallest

                        self.queue.pop(index_to_pop)

                        round_robin_processed_time += processing_time
                        current_time += rounds_to_complete_smallest
                        self.total_completion_time += current_time

            current_time += self.queue.pop().remaining_duration - round_robin_processed_time
            self.total_completion_time += current_time
            pbar.update(1)

    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)


if __name__ == "__main__":
    l = 0.5
    scheduler = PRR_scheduler(l, JobMeanOracle())
    numjobs = 100000
    filename = r"task_lines.txt"
    with open(filename, "r") as f:
        for i in tqdm(range(numjobs), "Job parsing"):
            a = [int(x) for x in f.readline().split(",")]
            scheduler.add_job(Job(a[1], a[0] // 1000000, a[2] // 1000000))
    # Running the scheduler
    tstart = time.time()
    scheduler.run()
    tend = time.time()
    print(
        f"total_completion_time: {sci_notation(scheduler.total_completion_time)} time: {tend - tstart}"
    )
