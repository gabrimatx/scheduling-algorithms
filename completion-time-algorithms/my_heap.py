from job_class import Job, PredictionClass
import random


class Heap:
    def __init__(self, elements):
        self.container = []
        for index, element in enumerate(elements):
            element.heap_index = index
            self.container.append(element)
        self.build_min_heap()

    def update_indexes(self):  # For debugging
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
        self.heapify(job_index)

    def pop_at_index(self, job_index):
        if job_index == len(self.container) - 1:
            return self.container.pop(-1)
        job_asked = self.container[job_index]
        self.container[job_index] = self.container.pop(-1)
        self.container[job_index].heap_index = job_index
        self.heapify(job_index)
        return job_asked

    def build_min_heap(self):
        n = len(self.container)
        for index in range(n // 2 + 1, -1, -1):
            self.heapify(index)

    def heapify(self, index):
        left_child = 2 * index + 1
        right_child = 2 * index + 2
        smallest = index

        if (
            left_child < len(self.container)
            and self.container[left_child] < self.container[smallest]
        ):
            smallest = left_child

        if (
            right_child < len(self.container)
            and self.container[right_child] < self.container[smallest]
        ):
            smallest = right_child

        if smallest != index:
            self.container[index], self.container[smallest] = (
                self.container[smallest],
                self.container[index],
            )
            self.container[index].heap_index = index
            self.container[smallest].heap_index = smallest
            self.heapify(smallest)


class PredictionHeap(Heap):
    def __init__(self, prediction_classes) -> None:
        super().__init__(prediction_classes)

    def update_prediction(self, prediction_class, new_amount):
        # Update the prediction class 
        prediction_class.prediction = new_amount
        self.heapify(prediction_class.heap_index)

    def empty_prediction_class(self, prediction_class: PredictionClass):
        # Send the class down the heap
        prediction_class.size_j = 0
        self.heapify(prediction_class.heap_index)

    def heap_push(self, prediction_class):
        # Found new class, add it to the prediction heap
        self.container.append(prediction_class)
        prediction_class.heap_index = len(self.container) - 1
        self.heapify_up(len(self.container) - 1)

    def heapify_up(self, index):
        while index > 0:
            parent_index = (index - 1) // 2
            if self.container[parent_index] < self.container[index]:
                break
            self.container[index], self.container[parent_index] = (
                self.container[parent_index],
                self.container[index],
            )
            self.container[index].heap_index = index
            self.container[parent_index].heap_index = parent_index
            index = parent_index


class HeapWithJobs(Heap):
    def __init__(self, elements) -> None:
        super().__init__(elements)
