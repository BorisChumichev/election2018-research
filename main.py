from preprocessing import load_stream_data, df_to_time_series
from indexer import run_indexer
from validator import run_validator


BASE_WINDOW = 3600 * 5
MAXIMUM_LAG = 20
INDEXER_HASH_BUCKETS = 5000
ACTIVITY_THRESHOLD = 3


vk_df = load_stream_data('data/vk.json')
timeseries = df_to_time_series(vk_df, BASE_WINDOW, 0)

suspicious_timeseries = run_indexer(
    timeseries,
    BASE_WINDOW,
    MAXIMUM_LAG,
    INDEXER_HASH_BUCKETS,
    ACTIVITY_THRESHOLD
)

# clusters = run_validator(suspicious_timeseries, MAXIMUM_LAG)

subsample = {}
for key, value in suspicious_timeseries.items():
    if (len(subsample) < 8):
        subsample[key] = value

clusters = run_validator(subsample, MAXIMUM_LAG)

print(clusters)