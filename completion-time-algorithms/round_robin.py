import heapq
from job_class import Job

class RR_scheduler:
    def __init__(self, time_quantumq):
        self.queue = []
        self.total_completion_time = 0
        self.quantum = time_quantum

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


time_quantum = int(input("Insert time quantum for round robin: "))

if __name__ == '__main__':
    scheduler = RR_scheduler(time_quantum)

    # Adding jobs
    numjobs = int(input("Insert number of jobs to process: "))
    filename = r"task_lines.txt"
    with open(filename, "r") as f:
        for i in range(numjobs):
            a = [int(x) for x in f.readline().split(",")]
            scheduler.add_job(Job(a[1], a[0]//1000, a[2]//1000))
    # Running the scheduler
    scheduler.run()
    print(f"total_completion_time: {scheduler.total_completion_time}")
