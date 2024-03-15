from job_class import Job
from scientific_not import sci_notation

class RR_scheduler:
    def __init__(self, time_quantum):
        self.queue = []
        self.total_completion_time = 0
        self.quantum = time_quantum

    def add_job(self, job):
        self.queue.append(job)

    def run(self):
        current_time = 0
        ordered_jobs = sorted(self.queue)
        min_index = 0
        while self.queue:
            # Run the scheduler in round robin fashion
            job_ind = 0
            queue_size = len(self.queue)
            minimum_round_size = ordered_jobs[min_index].remaining_duration
            if minimum_round_size > self.quantum:
                round_quantum = (minimum_round_size // self.quantum) * self.quantum
            else:
                round_quantum = self.quantum

            while job_ind < queue_size:
                if round_quantum >= self.queue[job_ind].remaining_duration:
                    current_time += (self.queue.pop(job_ind)).remaining_duration
                    self.total_completion_time += current_time
                    min_index += 1
                    queue_size -= 1
                else:
                    current_time += round_quantum
                    self.queue[job_ind].remaining_duration -= round_quantum
                    job_ind += 1


    def display_jobs(self):
        print("Current Jobs in Queue:")
        for job in self.queue:
            print(job)




if __name__ == '__main__':
    scheduler = RR_scheduler(0.000001)

    # Adding jobs
    numjobs = int(input("Insert number of jobs to process: "))
    filename = r"task_lines.txt"
    with open(filename, "r") as f:
        for i in range(numjobs):
            a = [int(x) for x in f.readline().split(",")]
            scheduler.add_job(Job(a[1], a[0]//1000000, a[2]//1000000))
    # Running the scheduler
    scheduler.run()
    print(f"total_completion_time: {sci_notation(scheduler.total_completion_time)}")
