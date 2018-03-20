import json
import pandas as pd
import numpy as np


def load_stream_data(filepath, format='vk'):
    """ Loads data from fs and returns pandas dataframe """   
    
    records = []
    with open(filepath, encoding='latin1') as f:
        print('Preprocessing: started reading {}'.format(filepath))

        if format == 'vk':
            for line in f:
                document = json.loads(line)
                records.append(
                    ( document['event']['creation_time']
                    , document['event']['action']
                    , document['event']['event_type']
                    , document['event']['tags']
                    , document['event']['author']['id']
                    , document['event']['text']
                    , document['event']['event_url']
                    )
                )
            df = pd.DataFrame(records, columns=['time', 'action', 'type', 'tags', 'author', 'text', 'url'])

        if format == 'twitter':
            for line in f:
                document = json.loads(line)
                records.append(
                    ( int(int(document['timestamp_ms']) / 1000)
                    , document['user']['screen_name']
                    , document['channels'].keys()
                    , document['text']
                    )
                )
            df = pd.DataFrame(records, columns=['time', 'author', 'tags', 'text'])

    print('Preprocessing: {} events loaded from fs'.format(df.shape[0]))
    return df


def df_to_time_series(df, time_window, offset = 0, verbose = True):
    """
    Converts pandas dataframe with 'time:int' and 'author:int' columns
    to a dict, where the key is author id and the value is timeseries
    of event counts for a given user at a given second
    """
    
    print('Preprocessing: converting to authors activities timeseries')
    
    # determine time bounds and filter out corresponding rows
    max_time = df.time.max() - int(time_window * offset)
    min_time = df.time.max() - int(time_window * (offset + 1))
    print('Preprocessing: time rage {} - {}'.format(min_time, max_time))
    time_window_df = df[df.time.apply(lambda t: t > min_time and t < max_time)]
    time_window_mathced_times = time_window_df.time.unique()
    
    # iterate over timepoints and construct dict with timeseries of
    # events by given author
    author_series = {}
    for time in range(min_time, max_time + 1):
        if not time in time_window_mathced_times:
            continue
        events_at_a_time = time_window_df[time_window_df['time'] == time]
        for author in events_at_a_time.author.unique():
            if not author in author_series:
                author_series[author] = np.zeros(max_time - min_time)
            events_at_a_time_by_author = events_at_a_time[events_at_a_time['author'] == author]
            author_series[author][time - min_time] = events_at_a_time_by_author.shape[0]
    
    print('Preprocessing: getting rid of authors with less than 2 activities')
    # filter out authors with just one activity, since correlating one activity is meaningless
    authors_to_delete = []
    for key, value in author_series.items():
        if sum(value) <= 1:
            authors_to_delete.append(key)
    for key in authors_to_delete:
        author_series.pop(key, None)
    
    return author_series
