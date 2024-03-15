from job_class import Job
import random
from math import log2

class heap:
    def __init__(self, elements):
        self.container = []
        for index, element in enumerate(elements):
            element.heap_index = index
            self.container.append(element)
        self.build_min_heap()

    def update_indexes(self): # For debugging
        for index, job in enumerate(self.container):
            job.heap_index = index

    def get_top(self):
        return self.container[0]

    def pop_head(self):
        if len(self.container) == 0:
            return None
        if len(self.container) == 1:
            return self.container.pop()

        min_val = self.container[0]
        self.container[0] = self.container.pop(-1)
        self.container[0].heap_index = 0
        self.heapify(0)

        return min_val

    def process_job(self, job_index, amount):
        process_time = min(self.container[job_index].remaining_duration, amount)
        self.container[job_index].remaining_duration -= process_time
        if self.container[job_index].remaining_duration >= 0:
            self.heapify(job_index)
        else: 
            raise BaseException
            

        return process_time
    
    def pop_at_index(self, job_index):
        if job_index == len(self.container) - 1:
            return self.container.pop(-1)
        job_asked = self.container[job_index]
        # print(job_index, self, self.container, sep = "\n")
        self.container[job_index] = self.container.pop(-1)
        self.container[job_index].heap_index = job_index
        self.heapify(job_index)
        return job_asked


    def build_min_heap(self):
        n = len(self.container)
        for index in range(n // 2 + 1, -1, -1):
            self.heapify(index)
        
        for index, job in enumerate(self.container):
            job.heap_index = index


    def heapify(self, index):
        left_child = 2 * index + 1
        right_child = 2 * index + 2
        smallest = index
        
        if left_child < len(self.container) and self.container[left_child] < self.container[smallest]:
            smallest = left_child
        
        if right_child < len(self.container) and self.container[right_child] < self.container[smallest]:
            smallest = right_child
        
        if smallest != index:
            self.container[index], self.container[smallest] = self.container[smallest], self.container[index]
            self.container[index].heap_index = index
            self.container[smallest].heap_index = smallest
            self.heapify(smallest)


    def __repr__(self):
        if not self.container:
            return "Heap is empty"

        lines = []
        depth = 0
        while 2 ** depth - 1 < len(self.container):
            start = 2 ** depth - 1
            end = min(2 ** (depth + 1) - 1, len(self.container))
            line = ' '.join(str(x.remaining_duration) for x in self.container[start:end])
            lines.append(line.center(80))
            depth += 1
        return '\n'.join(lines)



                

if __name__ == "__main__":
    l = [Job(i, 0, random.randint(0, 16)) for i in range(15)]
    my_heap = heap(l.copy())
    print(my_heap)
    print(my_heap.container)
    print(my_heap.pop_head())
    print(my_heap)
    print(my_heap.container)
    print(my_heap.pop_at_index(2))
    print(my_heap)
    print(my_heap.container)