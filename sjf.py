import heapq

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


class SPJFscheduler:
    def __init__(self):
        self.queue = []
        self.total_completion_time = 0

    def add_job(self, job):
        heapq.heappush(self.queue, job)

    def run(self):
        current_time = 0
        while self.queue:
            job = heapq.heappop(self.queue)
            # print(f"Processing {job} at time {current_time}")
            current_time += job.remaining_duration
            self.total_completion_time += current_time

    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)


# Example usage:
scheduler = SPJFscheduler()

# Adding jobs
numjobs = 500
for i in range(numjobs):
    a = [int(x) for x in input().split(",")]
    scheduler.add_job(Job(a[1], a[0]//10000, a[2]//10000))

# Displaying initial jobs in the queue
# scheduler.display_jobs()

# Running the scheduler
scheduler.run()
print(f"total_completion_time: {scheduler.total_completion_time}")
