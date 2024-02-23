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


class RR_scheduler:
    def __init__(self):
        self.queue = []
        self.total_completion_time = 0
        self.quantum = 1

    def add_job(self, job):
        self.queue.append(job)

    def run(self):
        current_time = 0
        completed_jobs = []
        while self.queue:
            # Run the scheduler in round robin fashion
            job_ind = 0
            queue_size = len(self.queue)
            while job_ind < queue_size:
                if self.quantum >= self.queue[job_ind].remaining_duration:
                    current_time += (self.queue.pop(job_ind)).remaining_duration
                    self.total_completion_time += current_time
                    queue_size -= 1
                else:
                    current_time += self.quantum
                    self.queue[job_ind].remaining_duration -= self.quantum
                    job_ind += 1

    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)


scheduler = RR_scheduler()

# Adding jobs
numjobs = 500
for i in range(numjobs):
    a = [int(x) for x in input().split(",")]
    scheduler.add_job(Job(a[1], a[0]//10000, a[2]//10000))

# Running the scheduler
scheduler.run()
print(f"total_completion_time: {scheduler.total_completion_time}")
