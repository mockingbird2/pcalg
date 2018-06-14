from multiprocessing import Pool

from itertools import combinations, permutations, chain
import logging

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
        sep_set = [[set() for i in range(self.node_size)] for j in
                   range(self.node_size)]
        remove_edges = []
        i, j = edges
        adj_i = list(self.graph.neighbors(i))
        if j not in adj_i:
            return True, sep_set, remove_edges
        else:
            adj_i.remove(j)
        if len(adj_i) >= self.level:
            _logger.debug('testing %s and %s' % (i, j))
            _logger.debug('neighbors of %s are %s' % (i, str(adj_i)))
            if len(adj_i) < self.level:
                return True, sep_set, remove_edges
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
                    sep_set[i][j] |= set(k)
                    sep_set[j][i] |= set(k)
                    break
            return True, sep_set, remove_edges
        return False, sep_set, remove_edges


def merge_sep_sets(sep_sets):
    sep_set = next(sep_sets)
    for sep in sep_sets:
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
    sep_sets = []
    while cont:
        task = Task(level, indep_test_func, data_matrix, kwargs,
                    stable, alpha, g)
        with Pool(10) as p:
            results = p.map(task.run, edges_permutations)
        conts, next_sep_sets, removable_edges = zip(*results)
        sep_sets.append(next_sep_sets)
        remove_edges = filter(None, removable_edges)
        cont = all(conts)
        level += 1
        if stable:
            g.remove_edges_from(chain(*remove_edges))
        if ('max_reach' in kwargs) and (level > kwargs['max_reach']):
            break
    sep_set = merge_sep_sets(chain(*sep_sets))
    return g, sep_set
