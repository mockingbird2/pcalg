from __future__ import print_function

from multiprocessing import Pool
from itertools import combinations, permutations
import logging

from pcalg import _create_complete_graph

_logger = logging.getLogger(__name__)


def splitted_task(level, indep_test_func, data_matrix, kwargs, stable, alpha,
                  remove_edges, g, sep_set):
    def task(edges):
        i, j = edges
        adj_i = list(g.neighbors(i))
        if j not in adj_i:
            return True, set()
        else:
            adj_i.remove(j)
        if len(adj_i) >= level:
            _logger.debug('testing %s and %s' % (i, j))
            _logger.debug('neighbors of %s are %s' % (i, str(adj_i)))
            if len(adj_i) < level:
                return True, set()
            for k in combinations(adj_i, level):
                _logger.debug('indep prob of %s and %s with subset %s'
                              % (i, j, str(k)))
                p_val = indep_test_func(data_matrix, i, j, set(k),
                                        **kwargs)
                _logger.debug('p_val is %s' % str(p_val))
                if p_val > alpha:
                    if g.has_edge(i, j):
                        _logger.debug('p: remove edge (%s, %s)' % (i, j))
                        if stable:
                            remove_edges.append((i, j))
                        else:
                            g.remove_edge(i, j)
                    sep_set[i][j] |= set(k)
                    sep_set[j][i] |= set(k)
                    break
        return True, sep_set
    return task


def estimate_skeleton_parallel(indep_test_func, data_matrix, alpha, **kwargs):
    def method_stable(kwargs):
        return ('method' in kwargs) and kwargs['method'] == "stable"
    stable = method_stable(kwargs)

    node_ids = range(data_matrix.shape[1])
    g = _create_complete_graph(node_ids)

    node_size = data_matrix.shape[1]
    sep_set = [[set() for i in range(node_size)] for j in range(node_size)]

    level = 0
    edges_permutations = list(permutations(node_ids, 2))
    while True:
        cont = False
        remove_edges = []
        for (i, j) in edges_permutations:
            adj_i = list(g.neighbors(i))
            if j not in adj_i:
                continue
            else:
                adj_i.remove(j)
            if len(adj_i) >= level:
                _logger.debug('testing %s and %s' % (i, j))
                _logger.debug('neighbors of %s are %s' % (i, str(adj_i)))
                if len(adj_i) < level:
                    continue
                for k in combinations(adj_i, level):
                    _logger.debug('indep prob of %s and %s with subset %s'
                                  % (i, j, str(k)))
                    p_val = indep_test_func(data_matrix, i, j, set(k),
                                            **kwargs)
                    _logger.debug('p_val is %s' % str(p_val))
                    if p_val > alpha:
                        if g.has_edge(i, j):
                            _logger.debug('p: remove edge (%s, %s)' % (i, j))
                            if stable:
                                remove_edges.append((i, j))
                            else:
                                g.remove_edge(i, j)
                        sep_set[i][j] |= set(k)
                        sep_set[j][i] |= set(k)
                        break
                cont = True
        level += 1
        if stable:
            g.remove_edges_from(remove_edges)
        if cont is False:
            break
        if ('max_reach' in kwargs) and (level > kwargs['max_reach']):
            break
    return (g, sep_set)
