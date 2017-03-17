import helper
import numpy as np
import pandas as pd

'''
Module for comparing the effect of different groups of factors on metrics.
'''

def create_factor_comparison_df(first_half, second_half, factor):
    '''
    Args:
        first_half (dataframe): this is a subset of the factor_df where the
        timestamps are less than or equal to the midpoint timestamp in the
        observed set
        second_half (dataframe): this is a subset of the factor_df where the
        timestamps are greater than the midpoint timestamp in the
        observed set
        factor (string): this is a string representation of the feature
        for which the label aggregated values are to be compared
    Returns:
        factor_comparison_df (dataframe): this is a dataframe where the
        columns are factor, first_half, second_half and the values are the
        labels for the factor, the sum total of the metric for the first half
        and the sum total of the metric for the second half
    To Do's:
        make this function work such that it is going to take in the main query
        and then at the same time identify what kind of metric it is such that
        if the metric is payout rate, the aggregator is going to be
        an average
    '''
    first_group = first_half.groupby(factor).sum().metric.reset_index()
    second_group = second_half.groupby(factor).sum().metric.reset_index()
    factor_comparison_df = first_group.merge(second_group, on=factor).fillna(0)
    factor_comparison_df.columns = ['factor', 'first_half', 'second_half']
    return factor_comparison_df

@helper.timeit
def get_main_factors(factor_df):
    '''
    Args:
        factor_df (dataframe): this is the culled dataframe which has the
        expanded factors added to it
    Returns:
        mainfactors_df (dataframe): this is a dataframe that has the
        information on the absolute value change of a factor over a time period
        by comparing the total in the first and second half of the time period
    '''
    timeperiods = np.unique(factor_df.tmstmp)
    n_timeperiods = len(timeperiods)
    mid = n_timeperiods / 2
    if n_timeperiods % 2 != 0:
        first_half = \
            factor_df[factor_df.tmstmp <= pd.to_datetime(timeperiods[mid])]
        second_half = \
            factor_df[factor_df.tmstmp > pd.to_datetime(timeperiods[mid])]
    else:
        first_half = \
            factor_df[factor_df.tmstmp < pd.to_datetime(timeperiods[mid])]
        second_half = \
            factor_df[factor_df.tmstmp >= pd.to_datetime(timeperiods[mid])]
    df_list = []
    factor_list = list(factor_df.columns)
    factor_list.remove('tmstmp')
    factor_list.remove('metric')
    for factor in factor_list:
        df_list.append(create_factor_comparison_df(first_half, second_half,
                                                   factor))
    mainfactors_df = pd.concat(df_list)
    mainfactors_df['abs_diff'] = \
        np.abs(mainfactors_df['second_half'] - mainfactors_df['first_half'])
    mainfactors_df = mainfactors_df.sort_values(by='abs_diff', ascending=False)
    mainfactors_df = mainfactors_df.reset_index()
    mainfactors_df.drop('index', axis=1, inplace=True)
    return mainfactors_df

@helper.timeit
def translate_mainfactors_df_into_list(mainfactors_df):
    '''
    Args:
        mainfactors_df (dataframe): this is a dataframe that has the
        information on the absolute value change of a factor over a time period
        by comparing the total in the first and second half of the time period
    Returns:
        mainfactors (list): this is a list of tuples where each element is made
        of three parts (factor, direction, absolute difference)
    '''
    mainfactors = []
    for row in xrange(mainfactors_df.shape[0]):
        factor = mainfactors_df.ix[row].factor
        first_half = mainfactors_df.ix[row].first_half
        second_half = mainfactors_df.ix[row].second_half
        direction = 'up' if second_half >= first_half else 'down'
        difference = round(second_half - first_half, 2)
        mainfactors.append((factor, direction, difference))
    return mainfactors

def find_factor_of_top_factor(top_factors, factor_df):
    '''
    Args:
        top_factors (str): this is the label identified in mainfactors
        as the label which caused the greatest absolute change in the
        aggregate data
        factor_df (dataframe): this is the culled dataframe which has the
        expanded factors added to it
    Returns:
        factor (string): this is a string representation of the feature
        for which the label aggregated values are to be compared
    '''
    factor_list = list(factor_df.columns)
    factor_list.remove('metric')
    factor_list.remove('tmstmp')
    for factor in factor_list:
        if top_factors in set(factor_df[factor]):
            return factor

def create_derivedmetrics():
    '''
    Args:
        None
    Returns:
        derivedmetrics (list): this is a list of tuples where each element
        is three items - the metric, the direction, and the percent change
    '''
    metrics = ['coin in', 'net wins', 'utility', 'coin out', 'jackpots']
    percent = np.random.rand(5) * np.random.choice([1, -1], 5)
    direction = ['up' if pct > 0 else 'down' for pct in percent]
    derivedmetrics = zip(metrics, direction, percent)
    return derivedmetrics

if __name__ == "__main__":
    pass
