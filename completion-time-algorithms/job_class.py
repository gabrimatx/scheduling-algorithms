class Job:
    def __init__(self, id: int, arrival_time: float, real_duration: float, heap_index = -1, queue_index = -1):
        self.id = id
        self.arrival_time = arrival_time
        self.real_duration = real_duration
        self.remaining_duration = real_duration
        self.prediction = 0
        self.heap_index = heap_index
        self.queue_index = queue_index
        
    def __repr__(self):
        return f"Job-{self.id}-({self.remaining_duration} units)"

    def __lt__(self, other):
        return self.remaining_duration < other.remaining_duration

class PredictionClass:
    def __init__(self, id: int, size_j: int, prediction: float, heap_index = -1) -> None:
        self.id = id
        self.size_j = size_j
        self.prediction = prediction
        self.heap_index = heap_index
    
    def __repr__(self) -> str:
        return f"class_id:{self.id} - jobs:{self.size_j} - pred:{self.prediction} - h_i:{self.heap_index}"
    
    def __lt__(self, other):
        # If class is empty, send it down the heap by giving it enormous weight
        good_size_self = float('inf') if self.size_j == 0 else 0
        good_size_other = float('inf') if other.size_j == 0 else 0
        return (self.prediction + good_size_self) < (other.prediction + good_size_other)

class JobBucket:
    def __init__(self, JobSet: list) -> None:
        self.buckets = {}
        for job in JobSet[::-1]:
            # Place the jobs in the bucket to execute the one arrived before first
            if job.id in self.buckets:
                self.buckets[job.id].append(job)
            else:
                self.buckets[job.id] = [job]

    def exec_job(self, bucket_id: int) -> Job:
        # Remove the job from the bucket and give it to the scheduler
        completed_job = self.buckets[bucket_id].pop()
        return completed_job
    
    def get_classes(self):
        # Return all classes of the buckets
        return set([key for key in self.buckets.keys() if self.buckets[key]])
    
    def pop_job(self, job):
        for ind, j in enumerate(self.buckets[job.id]):
            if j is job:
                return self.buckets[job.id].pop(ind)
        raise Exception
    
    # Methods to peek at selected bucket executing candidate
    def get_duration(self, bucket_id: int) -> int:
        return self.buckets[bucket_id][-1].remaining_duration

    def get_job(self, bucket_id: int) -> Job:
        return self.buckets[bucket_id][-1]
    
    # Check if bucket is empty
    def is_empty(self, bucket_id: int) -> bool:
        return len(self.buckets[bucket_id]) == 0
