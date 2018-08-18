import time
import networkx as nx
import datetime
from itertools import combinations


from skeletonmethods.pcalg import estimate_cpdag


def timer(runnable):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = runnable(*args, **kwargs)
        print('Run time: ', time.time() - start)
        return result

    return wrapper


class Pipeline:

    def __init__(self, data, test_data, estimation_params):
        self.data = data
        self.nodes = test_data['nodes']
        self.edges = test_data['edges']
        self.estimation_params = estimation_params
        self.skeletons = []

    @timer
    def build_skeleton(self, estimation_method):
        print('Running: ', estimation_method, datetime.datetime.now())
        return estimation_method(data_matrix=self.data, **self.estimation_params)

    def evaluate(self, estimation_method):
        skel, sep_set = self.build_skeleton(estimation_method)
        self.skeletons.append([skel, sep_set])
        # graph = estimate_cpdag(skel, sep_set)
        # test_graph = self.build_test_graph()
        return skel

    def build_test_graph(self):
        graph = nx.DiGraph()
        graph.add_nodes_from(self.nodes)
        graph.add_edges_from(self.edges)
        return graph

    def compare_result(self):
        if len(self.skeletons) < 2:
            print('Not enough graphs to compare')
        for comb in combinations(range(len(self.skeletons)), 2):
            firstSkel = self.skeletons[comb[0]][0]
            secondSkel = self.skeletons[comb[1]][0]
            firstSep = self.skeletons[comb[0]][1]
            secondSep = self.skeletons[comb[1]][1]
            yield comb, nx.is_isomorphic(firstSkel, secondSkel) and firstSep == secondSep

