class Job:
    def __init__(self, id, arrival_time, predicted_duration):
        self.id = id
        self.arrival_time = arrival_time
        self.predicted_duration = predicted_duration
        self.remaining_duration = predicted_duration
        
    def __repr__(self):
        return f"Job {self.id} ({self.remaining_duration} units remaining)"

    def __lt__(self, other):
        # Comparison based on predicted duration
        return self.remaining_duration < other.remaining_duration