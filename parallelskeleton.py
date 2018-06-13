from __future__ import print_function

from multiprocessing.pool import ThreadPool as Pool

from itertools import combinations, permutations, chain
import logging

from pcalg import _create_complete_graph

_logger = logging.getLogger(__name__)


def splitted_task(level, indep_test_func, data_matrix, kwargs, stable, alpha,
                  graph):
    node_size = data_matrix.shape[1]

    def task(edges):
        sep_set = [[set() for i in range(node_size)] for j in range(node_size)]
        remove_edges = []
        i, j = edges
        adj_i = list(graph.neighbors(i))
        if j not in adj_i:
            return True, sep_set, remove_edges
        else:
            adj_i.remove(j)
        if len(adj_i) >= level:
            _logger.debug('testing %s and %s' % (i, j))
            _logger.debug('neighbors of %s are %s' % (i, str(adj_i)))
            if len(adj_i) < level:
                return True, sep_set, remove_edges
            for k in combinations(adj_i, level):
                _logger.debug('indep prob of %s and %s with subset %s'
                              % (i, j, str(k)))
                p_val = indep_test_func(data_matrix, i, j, set(k),
                                        **kwargs)
                _logger.debug('p_val is %s' % str(p_val))
                if p_val > alpha:
                    if graph.has_edge(i, j):
                        _logger.debug('p: remove edge (%s, %s)' % (i, j))
                        if stable:
                            remove_edges.append((i, j))
                        else:
                            graph.remove_edge(i, j)
                    sep_set[i][j] |= set(k)
                    sep_set[j][i] |= set(k)
                    break
            return True, sep_set, remove_edges
        return False, sep_set, remove_edges

    return task

def merge_sep_sets(sep_sets):
    sep_set = sep_sets[0]
    for sep in sep_sets[1:]:
        for i in range(len(sep)):
            for j in range(len(sep[i])):
                sep_set[i][j] |= sep[i][j]
                sep_set[j][i] |= sep[j][i]
    return sep_set

def estimate_skeleton_parallel(indep_test_func, data_matrix, alpha, **kwargs):
    def method_stable(kwargs):
        return ('method' in kwargs) and kwargs['method'] == "stable"

    stable = method_stable(kwargs)

    node_ids = range(data_matrix.shape[1])
    g = _create_complete_graph(node_ids)

    level = 0
    edges_permutations = list(permutations(node_ids, 2))
    cont = True
    while cont:
        task = splitted_task(level, indep_test_func, data_matrix, kwargs,
                             stable, alpha, g)
        with Pool(10) as p:
            conts, sep_sets, removable_edges = zip(*p.map(task, edges_permutations))
        remove_edges = filter(None, removable_edges)
        cont = all(conts)
        level += 1
        if stable:
            g.remove_edges_from(chain(*remove_edges))
        if ('max_reach' in kwargs) and (level > kwargs['max_reach']):
            break
    sep_set = merge_sep_sets(sep_sets)
    return (g, sep_set)
