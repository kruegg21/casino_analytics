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
def sum_by_time(df, factor):
    if factor:
        return df.groupby(['tmstmp', factor], as_index = False).sum().rename(columns = {factor: 'factor'})
    else:
        return df.groupby(['tmstmp'], as_index = False).sum()

@timeit
def find_top_specific_factors(df, factor):
    return df.groupby([factor], as_index = False).sum().sort(columns = factor).rename(columns = {factor: 'factor'})

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
