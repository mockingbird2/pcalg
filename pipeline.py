import time
import networkx as nx

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

    @timer
    def build_skeleton(self, estimation_method):
        print('Running: ', estimation_method)
        return estimation_method(data_matrix=self.data, **self.estimation_params)

    def evaluate(self, estimation_method):
        skel, sep_set = self.build_skeleton(estimation_method)
        graph = estimate_cpdag(skel, sep_set)
        test_graph = self.build_test_graph()
        return nx.is_isomorphic(graph, test_graph)

    def build_test_graph(self):
        graph = nx.DiGraph()
        graph.add_nodes_from(self.nodes)
        graph.add_edges_from(self.edges)
        return graph
