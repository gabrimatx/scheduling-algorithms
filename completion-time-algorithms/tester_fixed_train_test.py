from rr import RR_scheduler
from sjf import SJF_scheduler
from spjf import SPJF_scheduler
from prr_optimized import PRR_scheduler
from ncs import NCS_scheduler
from oracles import GaussianPerturbationOracle, PerfectOracle, JobMeanOracle
from job_class import Job
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import random
import csv
import time

def moving_average(data, window_size):
    weights = np.repeat(1.0, window_size) / window_size
    return np.convolve(data, weights, 'valid')


def run_simulation(sample_size_train, sample_size_test, training_slice):
    

    pass