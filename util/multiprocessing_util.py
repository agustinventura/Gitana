#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'valerio cosentino'

import sys
sys.path.insert(0, "..//..")

import multiprocessing


def start_consumers(processes, task_queue, result_queue):
    num_consumers = processes
    consumers = [Consumer(task_queue, result_queue) for i in xrange(num_consumers)]
    for w in consumers:
        w.start()


def add_poison_pills(processes, task_queue):
    for i in xrange(processes):
        task_queue.put(None)


def get_tasks_intervals(elements, num_processes):
    chunk_size = len(elements) / num_processes
    if len(elements) % num_processes != 0:
        chunk_size += 1
    chunks = []
    if chunk_size == 0:
        chunks = [elements]
    else:
        for i in xrange(0, len(elements), chunk_size):
            chunks.append(elements[i:i + chunk_size])
    return chunks


class Consumer(multiprocessing.Process):

    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        while True:
            next_task = self.task_queue.get()
            if next_task is None:
                self.task_queue.task_done()
                break

            answer = next_task()
            self.task_queue.task_done()
            self.result_queue.put(answer)

        return
