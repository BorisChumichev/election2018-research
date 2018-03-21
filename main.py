from preprocessing import load_stream_data, df_to_time_series
from indexer import run_indexer
from validator import run_validator


BASE_WINDOW = 3600 * 6
MAXIMUM_LAG = 20
INDEXER_HASH_BUCKETS = 5000
ACTIVITY_THRESHOLD = 3

vk_df = load_stream_data('data/vk.json')

def run(offset):
    print('{} ====================='.format(offset))
    try:
        # timeseries = df_to_time_series(vk_df, BASE_WINDOW, offset)
        # suspicious_timeseries = run_indexer(
        #     timeseries,
        #     BASE_WINDOW,
        #     MAXIMUM_LAG,
        #     INDEXER_HASH_BUCKETS,
        #     ACTIVITY_THRESHOLD
        # )
        suspicious_timeseries = df_to_time_series(vk_df, BASE_WINDOW, offset)

        if (len(suspicious_timeseries) == 0):
            return print('')

        clusters = run_validator(suspicious_timeseries, MAXIMUM_LAG)
        print('Result: {}'.format(clusters))
    except:
        print('Error occured')
    

for offset in [0.5 * i for i in range(25)]:
    run(offset)