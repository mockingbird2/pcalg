import networkx as nx
import numpy as np
from pyBN.classes.bayesnet import BayesNet


def direct_edges(graph):
    g = nx.DiGraph()
    for edge in graph.edges():
        start, end = edge
        if start < end:
            g.add_edge(start, end)
        elif start > end:
            g.add_edge(end, start)
        else:
            continue
    return g


def generate_edge_dict(graph):
    edge_dict = {}
    for e in nx.generate_adjlist(graph):
        e_list = [int(x) for x in e.split(' ')]
        edge_dict[e_list[0]] = e_list[1:]
    return edge_dict


def generate_value_dict(data):
    """
    Assuming data.shape[0] are the observations and data.shape[1] are the
    variables
    """
    n_rv = data.shape[1]
    value_dict = dict(
        zip(range(n_rv), [list(np.unique(col)) for col in data.T]))
    return value_dict


def build_bayesnet(graph, data):
    directed = direct_edges(graph)
    edge_dict = generate_edge_dict(directed)
    print(edge_dict)
    value_dict = generate_value_dict(data)
    print(value_dict)
    return BayesNet(edge_dict, value_dict)
