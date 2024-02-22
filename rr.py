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


class RRscheduler:
    def __init__(self):
        self.queue = []
        self.total_completion_time = 0
        self.quantum = 100

    def add_job(self, job):
        self.queue.append(job)

    def run(self):
        current_time = 0
        flag = True
        while self.queue:
            if not flag:
                break
            flag = False
            for job_i in range(len(self.queue)):
                if self.queue[job_i].id == -1:
                    continue
                flag = True
                # print(f"Processing {self.queue[job_i]} at time {current_time}")
                if self.quantum >= self.queue[job_i].remaining_duration:
                    current_time += min(self.quantum, self.queue[job_i].remaining_duration)
                    self.total_completion_time += current_time
                    self.queue[job_i].id = -1
                else:
                    current_time += self.quantum
                    self.queue[job_i].remaining_duration -= self.quantum




    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)


# Example usage:
scheduler = RRscheduler()

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
