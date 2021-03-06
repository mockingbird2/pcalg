import logging
from itertools import combinations, chain
from multiprocessing import Pool

import networkx as nx
import datetime

from skeletonmethods.pcalg import _create_complete_graph

_logger = logging.getLogger(__name__)


class Task:
    def __init__(self, level, indep_test_func, data_matrix, kwargs, stable,
                 alpha, graph):
        self.level = level
        self.indep_test_func = indep_test_func
        self.data_matrix = data_matrix
        self.node_size = data_matrix.shape[1]
        self.kwargs = kwargs
        self.stable = stable
        self.alpha = alpha
        self.graph = graph

    def run(self, edges):
        sep_set = []
        remove_edges = []
        i, j = edges
        adj_i = list(self.graph.neighbors(i))
        if j not in adj_i:
            return False, sep_set, remove_edges
        else:
            adj_i.remove(j)
        if len(adj_i) >= self.level:
            _logger.debug('testing %s and %s' % (i, j))
            _logger.debug('neighbors of %s are %s' % (i, str(adj_i)))
            if len(adj_i) < self.level:
                return False, sep_set, remove_edges
            for k in combinations(adj_i, self.level):
                _logger.debug('indep prob of %s and %s with subset %s'
                              % (i, j, str(k)))
                p_val = self.indep_test_func(self.data_matrix, i, j, set(k),
                                             **self.kwargs)
                _logger.debug('p_val is %s' % str(p_val))
                if p_val > self.alpha:
                    if self.graph.has_edge(i, j):
                        _logger.debug('p: remove edge (%s, %s)' % (i, j))
                        if self.stable:
                            remove_edges.append((i, j))
                        else:
                            self.graph.remove_edge(i, j)
                    sep_set.append((i, j, set(k)))
                    sep_set.append((j, i, set(k)))
                    break
            return True, sep_set, remove_edges
        return False, sep_set, remove_edges


def merge_sep_sets(size, sep_sets):
    result = [[set() for i in range(size)] for j in range(size)]
    for sep_set in sep_sets:
        if sep_set == []:
            continue
        for x in sep_set:
            i = x[0]
            j = x[1]
            k = x[2]
            result[i][j] |= k
    return result


def estimate_skeleton_parallel(indep_test_func, data_matrix, alpha, **kwargs):
    def method_stable(kwargs):
        return ('method' in kwargs) and kwargs['method'] == "stable"

    stable = method_stable(kwargs)

    node_ids = range(data_matrix.shape[1])
    g = _create_complete_graph(node_ids)

    level = 0
    cont = True
    sep_sets = []
    while cont:
        edges_permutations = list(g.edges()) + [x[::-1] for x in list(g.edges())]
        task = Task(level, indep_test_func, data_matrix, kwargs,
                    stable, alpha, g)
        with Pool(10) as p:
            results = p.map(task.run, edges_permutations)
        conts, next_sep_sets, removable_edges = zip(*results)
        sep_sets.extend(next_sep_sets)
        remove_edges = filter(None, removable_edges)
        cont = any(conts)
        level += 1
        if stable:
            g.remove_edges_from(chain(*remove_edges))
        if ('max_reach' in kwargs) and (level > kwargs['max_reach']):
            break
    sep_set = merge_sep_sets(data_matrix.shape[1], sep_sets)
    return g, sep_set


def estimate_skeleton_naive(indep_test_func, data_matrix, alpha, **kwargs):
    node_ids = range(data_matrix.shape[1])
    complete_graph = _create_complete_graph(node_ids)
    g, sep_sets = estimate_skeleton_naive_step(indep_test_func, data_matrix, alpha, 0, complete_graph, **kwargs)
    sep_set = merge_sep_sets(data_matrix.shape[1], sep_sets)
    return g, sep_set


def estimate_skeleton_naive_step(indep_test_func, data_matrix, alpha, level, g, **kwargs):
    def method_stable(kwargs):
        return ('method' in kwargs) and kwargs['method'] == "stable"

    stable = method_stable(kwargs)

    if nx.number_of_edges(g) == 0:
        print("level", level, ":", 1, "subgraphs")
        return g, None

    cont = True
    sep_sets = []
    while cont:
        edges_permutations = list(g.edges()) + [x[::-1] for x in list(g.edges())]
        task = Task(level, indep_test_func, data_matrix, kwargs,
                    stable, alpha, g)
        with Pool(10) as p:
            results = p.map(task.run, edges_permutations)
        conts, next_sep_sets, removable_edges = zip(*results)
        sep_sets.extend(next_sep_sets)
        remove_edges = filter(None, removable_edges)
        cont = any(conts)
        level += 1
        if stable:
            g.remove_edges_from(chain(*remove_edges))

            # additional subgraph handling
            subgraphs = list(nx.connected_component_subgraphs(g))
            
            print("level", level-1, ":", len(subgraphs), "subgraphs")
            
            if len(subgraphs) > 1:
                graphs = []
                for x in subgraphs:
                    cur_g, cur_sep_set = estimate_skeleton_naive_step(indep_test_func, data_matrix, alpha, level, x,
                                                                      **kwargs)

                    print("Results:", datetime.datetime.now(), len(cur_g))
                    graphs.append(cur_g)
                    if cur_sep_set is not None:
                        sep_sets.extend(cur_sep_set)
                return nx.union_all(graphs), sep_sets

        if ('max_reach' in kwargs) and (level > kwargs['max_reach']):
            break
    return g, sep_sets
