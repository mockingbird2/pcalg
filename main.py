import numpy as np
from gsq.gsq_testdata import bin_data

from pipeline import Pipeline
from skeletonmethods import estimate_skeleton_parallel, estimate_skeleton
from skeletonmethods.indeptests import partial_corr_test

if __name__ == '__main__':
    test_data = {
        'nodes': [0, 1, 2, 3, 4],
        'edges': [(0, 1), (2, 3), (3, 2), (3, 1),
                  (2, 4), (4, 2), (4, 1)]
    }
    estimation_params = {
        'indep_test_func': partial_corr_test,
        'alpha': 0.01,
        'method': 'stable'
    }
    config = {
        'data': np.array(bin_data).reshape((5000, 5)),
        'estimation_params': estimation_params,
        'test_data': test_data
    }
    pipeline = Pipeline(**config)
    correct = pipeline.evaluate(estimate_skeleton_parallel)
    print('Correctness: ', correct)
    correct = pipeline.evaluate(estimate_skeleton)
    print('Correctness: ', correct)
