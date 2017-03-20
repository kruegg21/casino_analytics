from sqlalchemy import create_engine
from datetime import timedelta, datetime
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
def get_sql_data(query, engine, in_memory = False):
    '''
    Add in memory capability
    '''
    if in_memory:
        pass
    else:
        return pd.read_sql_query(query, con = engine)

@timeit
def sum_by_time(df, factor, pupd = True):
    if factor:
        return df.groupby(['tmstmp', factor], as_index = False).sum().rename(columns = {factor: 'factor'})
    else:
        return df.groupby(['tmstmp'], as_index = False).sum()

@timeit
def find_top_specific_factors(df, factor, query_params):
    '''
    Clean this god awful function up when you get the chance
    '''
    if factor[:3] == 'top' or factor[:5] == 'worst':
        # Our factor is a time factor
        df_1 = df.set_index(pd.DatetimeIndex(df['tmstmp']))
        if factor == 'top month':
            resample_string = 'M'
            # Make more exact
            adjustment = 31
        if factor == 'top week':
            resample_string = 'W'
            adjustment = 7
        if factor == 'top day' or factor == 'worst day':
            resample_string = '1D'
            adjustment = 1
        if factor == 'top hour':
            resample_string = '60Min'
            adjustment = float(1) / 24
        if factor == 'top minute':
            resample_string = '1Min'
            adjustment = float(1) / 1440

        # Resample
        df_1 = df_1.resample(resample_string).sum().sort_values('metric', ascending = True)
        df_1['factor'] = df_1.index
        df_1['factor'] = df_1.factor.dt.strftime('%Y-%m-%d')

        # Adjust metric for PUPD calculation
        df_1['metric'] = (df_1['metric'] * query_params.days_per_interval) / float(adjustment)

        return df_1
    return df.groupby([factor], as_index = False).sum().sort_values('metric', ascending = True).rename(columns = {factor: 'factor'})

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

@timeit
def calculate_pupd(df, query_params):
    '''
    Calculates the PUPD (per unit per day) value for column 'metric' in df,
    replacing the column 'metric' with this calculated value.
    Input:
        df -- DataFrame
        query_params -- query_parameters object
    '''
    df['total'] = df.metric
    df['metric'] = df.metric / (query_params.days_per_interval * query_params.num_machines)
    return df

def convert_money_to_string(f):
    '''
    Turns dollar float into human readable string
    '''

    return '$' + str(format(round(f, 3), ",.3f"))

def convert_datetime_to_string(dt, period):
    '''
    Turns python datetime object into the correct string for a given query
    parameter
    Input:
        dt (datetime) -- datetime object
        period (str) -- string of the period we want to concatenate datetime to
    '''
    s = ''
    if period == 'year':
        s = datetime.strftime(dt, "%Y")
    elif period == 'month':
        s = datetime.strftime(dt, "%B %Y")
    elif period == 'quarter':
        quarter = (dt.month-1)//3
        year = datetime.strftime(dt, "%Y")
        if quarter == 0:
            quarter_string = 'First'
        elif quarter == 1:
            quarter_string = 'Second'
        elif quarter == 2:
            quarter_string = 'Third'
        else:
            quarter_string = 'Fourth'
        s = quarter_string + ' Quarter of ' + year
    elif period == 'week':
        s = 'Week of ' + datetime.strftime(dt, "%Y-%m-%d")
    elif period == 'day':
        s = datetime.strftime(dt, "%Y-%m-%d")
    elif period == 'hour':
        s = datetime.strftime(dt, "%Y-%m-%d %H:00:00")
    elif period == 'minute':
        s = datetime.strftime(dt, "%Y-%m-%d %H:%M:00")
    else:
        pass
    return s

@timeit
def filter_by_specific_factor(df, factor, specific_factor):
    df_filtered = df[df[factor] == specific_factor]
    return df.groupby(['tmstmp', factor], as_index = False).sum().rename(columns = {factor: 'factor'})

if __name__ == "__main__":
    dt = datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p')
    period = 'minute'
    convert_datetime_to_string(dt, period)
