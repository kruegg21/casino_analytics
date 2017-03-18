from sqlalchemy import create_engine
from datetime import timedelta
import pandas as pd
import time

# Timing function
def timeit(method):
    """
    Timing wrapper
    """
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print 'Running %r took %2.4f sec\n' % \
              (method.__name__, te-ts)
        return result
    return timed

@timeit
def connect_to_database(user, domain, name):
    engine = create_engine('postgresql://{}:{}/{}'.format(user, domain, name))
    return engine

@timeit
def get_sql_data(query, engine):
    return pd.read_sql_query(query, con = engine)

@timeit
def sum_by_time(df, factor, pupd = True):
    if factor:
        return df.groupby(['tmstmp', factor], as_index = False).sum().rename(columns = {factor: 'factor'})
    else:
        return df.groupby(['tmstmp'], as_index = False).sum()

@timeit
def find_top_specific_factors(df, factor):
    '''
    Clean this god awful function up when you get the chance
    '''
    if factor[:3] == 'top':
        # Our factor is a time factor
        df_1 = df.set_index(pd.DatetimeIndex(df['tmstmp']))
        if factor == 'top month':
            resample_string = 'M'
        if factor == 'top week':
            resample_string = 'W'
        if factor == 'top day':
            resample_string = '1D'
        if factor == 'top hour':
            resample_string = '60Min'
        if factor == 'top minute':
            resample_string = '1Min'

        # Resample
        df_1 = df_1.resample(resample_string).sum().sort_values('metric')
        df_1['factor'] = df_1.index
        df_1['factor'] = df_1.factor.dt.strftime('%Y-%m-%d')
        return df_1
    return df.groupby([factor], as_index = False).sum().sort_values('metric').rename(columns = {factor: 'factor'})

def round_timedelta(td, period):
    """
    Rounds the given timedelta by the given timedelta period
    :param td: `timedelta` to round
    :param period: `timedelta` period to round by.
    """
    period_seconds = period.total_seconds()
    half_period_seconds = period_seconds / 2
    remainder = td.total_seconds() % period_seconds
    if remainder >= half_period_seconds:
        return timedelta(seconds=td.total_seconds() + (period_seconds - remainder))
    else:
        return timedelta(seconds=td.total_seconds() - remainder)

def get_number_days(query_params):
    '''
    Finds the number of days spanning the query
    Input:
        query_params -- query parameters object
    Output:
        integer of the number of days spanning the query
    '''
    num_days = query_params.stop - query_params.start
    return round_timedelta(num_days, timedelta(days = 1)).days

def calculate_pupd(df, query_params):
    '''
    Calculates the PUPD (per unit per day) value for column 'metric' in df,
    replacing the column 'metric' with this calculated value.
    Input:
        df -- DataFrame
        query_params -- query_parameters object
    '''
    df['metric'] = df.metric / (query_params.days_per_interval * query_params.num_machines)
    return df
