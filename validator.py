from functools import partial
import pandas as pd
import numpy as np
import multiprocessing as mp
from cdtw import pydtw
from scipy.stats import zscore
from scipy.cluster.hierarchy import dendrogram, linkage, fcluster
from sklearn.metrics.pairwise import pairwise_distances


def warped_correlation(window_size, signals):
    X = signals[0]
    Y = signals[1]
    cdtw = pydtw.dtw(
        zscore(X),
        zscore(Y),
        pydtw.Settings(step = 'dp1',
            window = 'scband',
            param = window_size,
            norm = False,
            compute_path = True
        )
    )
    dist = cdtw.get_dist()
    path = len(cdtw.get_path())
    warped_corellation = 1 - dist / (2 * path)
    return warped_corellation


def compute_pairwise_condenced(items, func):
    item_pairs = []

    for j in range(len(items)):
        for i in range(len(items)):
            if (i > j):
                item_pairs.append([items[j], items[i]])

    pool = mp.Pool(processes=mp.cpu_count())
    return pool.map(func, item_pairs)


def filter_false_positives(clusters):
    filtered = []
    for key, cluster in clusters.items():
        if len(cluster) != 1:
            filtered.append(cluster)
    return filtered


def run_validator(timeseries, window_size):
    authors = []
    activities = []
    for author, series in timeseries.items():
        authors.append(author)
        activities.append(series)
    
    print('Validator: computing cDTW distance matrix on {} CPUs'.format(mp.cpu_count()))
    constrained_warped_corellation = partial(warped_correlation, window_size)
    
    condensed_distance_matrix = compute_pairwise_condenced(
        activities,
        constrained_warped_corellation
    )
    
    print('Validator: clustering')
    linkage_matrix = linkage(abs(np.array(condensed_distance_matrix) - 1), method='single')
    #dendrogram(linkage_matrix)
    cluster_indexes = fcluster(linkage_matrix, 0.005, 'distance')
    

    clusters = {}
    for cluster_index in cluster_indexes:
        clusters[cluster_index] = []

    for i in range(len(authors)):
        clusters[cluster_indexes[i]].append(authors[i])

    print('Validator: done')
    return filter_false_positives(clusters)
