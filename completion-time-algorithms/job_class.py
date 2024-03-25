class Job:
    def __init__(self, id: int, arrival_time: float, real_duration: float, heap_index = -1, queue_index = -1):
        self.id = id
        self.arrival_time = arrival_time
        self.real_duration = real_duration
        self.remaining_duration = real_duration
        self.oracle_prediction = -1
        self.heap_index = heap_index
        self.queue_index = queue_index
        
    def __repr__(self):
        return f"Job-{self.id}-({self.remaining_duration} units)"

    def __lt__(self, other):
        # Comparison based on predicted duration
        return self.remaining_duration < other.remaining_duration