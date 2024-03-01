from job_class import Job
from scientific_not import sci_notation
import time

class RR_scheduler:
    def __init__(self, time_quantumq):
        self.queue = []
        self.total_completion_time = 0
        self.quantum = time_quantum

    def add_job(self, job):
        self.queue.append(job)

    def run(self):
        start = time.time()
        current_time = 0
        self.queue = self.queue[(len(self.queue) // 100 * 20):]
        ordered_jobs = sorted(self.queue)
        min_index = 0
        total_quantum_increases = 0
        rounds = 0
        while self.queue:
            # Run the scheduler in round robin fashion
            rounds += 1
            job_ind = 0
            queue_size = len(self.queue)
            minimum_round_size = ordered_jobs[min_index].remaining_duration
            if minimum_round_size > self.quantum:
                round_quantum = (minimum_round_size // self.quantum) * self.quantum
                total_quantum_increases += minimum_round_size // self.quantum
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

        end = time.time()
        print(f"Time used {end - start} Increases {total_quantum_increases} Rounds: {rounds}")

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
    print(f"total_completion_time: {sci_notation(scheduler.total_completion_time)}")
