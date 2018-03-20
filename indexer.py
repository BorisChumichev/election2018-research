import numpy as np
from scipy.stats import pearsonr, zscore
from math import floor


def cross_correlation(X, Y, lag_size):
    lags = [ i - 5 for i in range(lag_size * 2 + 1) ]
    series_length = len(X)
    
    cross_correlation = []
    
    for lag in lags:
        if lag >= 0:
            cross_correlation.append(
                pearsonr(
                    X[0:series_length - lag],
                    Y[lag:series_length]
                )[0]
            )
        if lag < 0:
            cross_correlation.append(
                pearsonr(
                    X[abs(lag):series_length],
                    Y[0:series_length - abs(lag)]
                )[0]
            )
            
    return cross_correlation


def build_random_projections(timeseries, base_window, maximum_lag):
    projections = {}
    
    random_vector = zscore(np.random.randint(20, size=base_window))

    for author, timeseries in timeseries.items():
        projections[author] = cross_correlation(
            random_vector,
            zscore(timeseries),
            maximum_lag
        )
    
    return projections


def bucketize_projections(projections, buckets_count):
    # compute index ranges and bucket size
    projection_range = [0, 0]
    for author, projection in projections.items():
        if min(projection) < projection_range[0]:
            projection_range[0] = min(projection)
        if max(projection) > projection_range[1]:
            projection_range[1] = max(projection)
    bucket_size = (projection_range[1] - projection_range[0]) / buckets_count
    
    # distribute authors between buckets
    buckets = {}
    
    for bucket in range(buckets_count + 1):
        buckets[bucket] = []
    
    for author, projection in projections.items():
        for value in projection:
            buckets[floor((value - projection_range[0]) / bucket_size)].append(author)
    
    return buckets


def extract_suspicious_authors(buckets, threshold):
    suspicious_authors = []
    
    for bucket, contents in buckets.items():
        qualified_authors = []
        
        for author in np.unique(contents):
            if (contents.count(author) >= threshold):
                qualified_authors.append(author)
        
        if (len(qualified_authors) >= threshold):
            for qualified_author in qualified_authors:
                if (qualified_author not in suspicious_authors):
                    suspicious_authors.append(qualified_author)
    
    return suspicious_authors


def suspicious_timeseries_only(timeseries, suspects, activity_threshold):
    """ Remains records only for matched authors """
    
    suspicious_timeseries = {}
    
    for suspect in suspects:
        if (sum(timeseries[suspect]) >= activity_threshold):
            suspicious_timeseries[suspect] = timeseries[suspect]
    
    return suspicious_timeseries


def run_indexer(timeseries, base_window, maximum_lag, buckets_count, activity_threshold):
    print('Indexer: started indexing {} author activities within {} hours window'.format(len(timeseries), base_window/60/60))
    print('Indexer: using maximum lag of {} and {} hash buckets'.format(maximum_lag, buckets_count))
    print('Indexer: building random projections'.format(len(timeseries)))
    projections = build_random_projections(timeseries, base_window, maximum_lag)
    print('Indexer: bucketizing authors')
    author_buckets = bucketize_projections(projections, buckets_count)
    print('Indexer: extracting suspicious users')
    suspects = extract_suspicious_authors(author_buckets, floor(maximum_lag / 4))
    print('Indexer: {} suspects found'.format(len(suspects)))
    suspicious_timeseries = suspicious_timeseries_only(timeseries, suspects, activity_threshold)
    print('Indexer: {} suspects remained after applying activity threshold of {}'.format(len(suspicious_timeseries), activity_threshold))
    return suspicious_timeseries
