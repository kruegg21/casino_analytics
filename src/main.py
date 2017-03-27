import helper
import json
import pandas as pd
import mpld3
import numpy as np
import requests
import factor_analysis
import visualizations
from datetime import timedelta, datetime
from netwin_analysis import netwin_analysis
from sqlalchemy import create_engine
from generateresponsefromrequest import get_intent_entity_from_watson
from query_parameters import query_parameters
from translation_dictionaries import *

# Read password from external file
with open('passwords.json') as data_file:
    data = json.load(data_file)

DATABASE_HOST = 'soft-feijoa.db.elephantsql.com'
DATABASE_PORT = '5432'
DATABASE_NAME = 'ohdimqey'
DATABASE_USER = 'ohdimqey'
DATABASE_PASSWORD = data['DATABASE_PASSWORD']

# Connect to database
database_string = 'postgres://{}:{}@{}:{}/{}'.format(DATABASE_USER,
                                                     DATABASE_PASSWORD,
                                                     DATABASE_HOST,
                                                     DATABASE_PORT,
                                                     DATABASE_NAME)
engine = create_engine(database_string)
main_factors = ['bank', 'zone', 'clublevel', 'area']
specific_factors = ['club_level', 'area', 'game_title', 'manufacturer',
                    'stand', 'zone', 'bank']

# dataframes = {}

def impute_period(query_params, error_checking = False):
    '''
    Checks to see if a period is specified in query parameters object. If none
    is specified, this function imputes a period by looking at the range in the
    query parameters object. The imputed range is then put into the sql_period
    attribute of the query params object.
    Input:
        query_params -- query parameters object
        error_checking (bool) -- whether to print to console
    Output:
        query_params with imputed period
    '''
    # Check to see if a period is specified, if not impute period based on range
    if not query_params.period:
        period = None
        time_range = query_params.stop - query_params.start
        if time_range > timedelta(hours = 23, minutes = 59, seconds = 59):
            # Range is greater than a day
            if time_range > timedelta(days = 6, hours = 23, minutes = 59, seconds = 59):
                # Range is greater than a week
                if time_range > timedelta(days = 31, hours = 23, minutes = 59, seconds = 59):
                    # Range is greater than a month
                    if time_range > timedelta(days = 364, hours = 23, minutes = 59, seconds = 59):
                        # Range is greater than a year
                        # Segment by months
                        period = 'monthly'
                    else:
                        # Range is less than a year
                        # Segment by weeks
                        period = 'weekly'
                else:
                    # Range is less than a month
                    # Segment by days
                    period = 'daily'
            else:
                # Range is less than week
                # Segment by hour
                period = 'hourly'
        else:
            # Segment by minute
            period = 'by_minute'

        # Add imputed period
        query_params.sql_period = translation_dictionary[period]

    # Check to see if we need more granularity for time factor
    if query_params.time_factor:
        if query_params.time_factor == 'top minute':
            if query_params.sql_period in ['year', 'month', 'week', 'day', 'hour']:
                query_params.sql_period = 'minute'
        if query_params.time_factor == 'top hour':
            if query_params.sql_period in ['year', 'month', 'week', 'day']:
                query_params.sql_period = 'hour'
        if query_params.time_factor == 'top day':
            if query_params.sql_period in ['year', 'month', 'week']:
                query_params.sql_period = 'day'
        if query_params.time_factor == 'top week':
            if query_params.sql_period in ['year', 'month']:
                query_params.sql_period = 'week'
        if query_params.time_factor == 'top month':
            if query_params.sql_period in ['year']:
                query_params.sql_period = 'month'

    return query_params

def get_data_from_nl_query(nl_query, error_checking = False):
    '''
    Input:
        nl_query (str) -- this is a natural language query
                          i.e. what is my revenue today
    Returns
        df (dataframe) -- this is a pandas dataframe that contains a table
                          which will be used for visualization
        query_params (query_parameters object) -- this is an object holding
                                                  everything we need to know
                                                  about the query
    '''

    # Get JSON Watson conversations response to natual language query
    response = get_intent_entity_from_watson(nl_query, error_checking = False)

    # Transform JSON Watson conversations response to query parameters object
    query_params = query_parameters()
    query_params.generate_query_params_from_response(nl_query, response, error_checking = error_checking)

    # Add main factors
    if query_params.intent == 'machine_performance':
        pass
    else:
        query_params.sql_factors += main_factors

    # Impute period if needed
    query_params = impute_period(query_params)

    # Generate SQL query
    query_params.generate_sql_query(error_checking = error_checking)

    # Get SQL query string from query parameters object
    sql_query = query_params.sql_string
    if error_checking:
        print query_params

    # Place SQL results into DataFrame
    df = helper.get_sql_data(sql_query, engine)
    if error_checking:
        print df.head()

    return df, query_params

def main(query, error_checking = False):
    '''
    Args:
        query (str): this is the natural language input string
    Returns:
        plot1 (unicode): this is the html, css, javascript to render the
        mpld3 plot
        mainfactors (list): this is a list of tuples where each element
        is three items - the metric, the direction, and the percent change
        plot2 (unicode): this is the html, css, javascript to render the
        mpld3 plot
        derivedmetrics (list): this is a list of tuples where each element
        is three items - the metric, the direction, and the percent change
        aggregate_statistics (dict) -- dictionary of aggregate statistics to
                                       display on dashboard
    '''
    # Pull down data from database
    df, query_params = get_data_from_nl_query(query, error_checking = error_checking)

    # Decide what to do based on query parameters
    """
    # Metric
    self.metric = None

    # Factor(s)
    self.factors = []

    # Range
    self.start = datetime.strptime('2015-01-01', '%Y-%m-%d')
    self.stop = datetime.strptime('2015-01-02', '%Y-%m-%d')

    # Period
    self.period = None

    # Ordering
    self.ordering = 'date'

    # Aggregate Statistic
    self.statistic = None

    # Specific Factors
    self.club_level = None
    self.area = None
    self.game_title = None
    self.manufacturer = None
    self.stand = None
    self.zone = None
    self.bank = None
    """
    # All queries will have a metric and range (if none provided we will infer)
    # M: always included (if not infer)
	# F: indicates multi-line graph or multi-dimensional histogram
    	# SF: indicates filtering
	# R: always included (if not infer)
	# P: indicates which materialized view to pull from, if missing indicates a
    #    single value answer should be provided
	# O: indicates histogram
	# S:

    # Dictionary to hold calculated metrics
    metrics = {}
    print query_params

    # Check if we want to do net win analysis
    if query_params.intent == 'netwin_analysis':
        return netwin_analysis(df, query_params, engine)

    # Determine metrics and graph type to build
    if query_params.ordering == 'date' and query_params.intent != 'machine_performance':
        # Line graph

        # Find factor we need to aggregate on (currently supports only single factor)
        if query_params.factors:
            factor = translation_dictionary.get(query_params.factors[0], query_params.factors[0])
        else:
            factor = None
        df_1 = helper.sum_by_time(df, factor)

        # Calculate number of days
        query_params.num_days = len(df_1.tmstmp.unique()) * query_params.days_per_interval

        # Calculate metric total we are interested in
        if factor:
            # Multiple factor
            total_metric_for_specific_factors = df_1.groupby(['factor'], as_index = False).sum()
            for index, row in total_metric_for_specific_factors.iterrows():
                # Calculate metric PUPD
                metric_per_day_name = "{} for {}".format(human_readable_translation[query_params.sql_metric],
                                                         human_readable_translation[row['factor']])
                metrics[metric_per_day_name] = round(row.metric / (query_params.num_days * query_params.num_machines), 3)
        else:
            # Single total
            total_metric = df_1['metric'].sum()

            # Calculate metric PUPD
            metric_per_day_name = "{}".format(human_readable_translation[query_params.sql_metric])
            metrics[metric_per_day_name] = round(total_metric / (query_params.num_days * query_params.num_machines), 3)

        # Calculate PUPD for each metric
        df_1 = helper.calculate_pupd(df_1, query_params)

        # Round to 3 decimal places
        df_1 = df_1.round(3)

        # Make Plot
        text = 'hello'
        plot1 = visualizations.makeplot('line', df_1, query_params, metrics)
    else:
        # Bar plot

        # Find factor (currently supports one factor)
        if query_params.factors:
            factor = translation_dictionary.get(query_params.factors[0], query_params.factors[0])
        else:
            # Defaults to clublevel
            factor = 'clublevel'

        if query_params.time_factor:
            factor = query_params.time_factor

        # Find top specific factors for given factor
        df_1 = helper.find_top_specific_factors(df, factor, query_params)

        # Calculate PUPD for each metric
        if query_params.show_as_pupd:
            df_1 = helper.calculate_pupd(df_1, query_params)

        # Find metrics to display
        if query_params.ordering == 'best' or query_params.intent == 'machine_performance':
            best = df_1.iloc[-1]['factor']
            metric_for_best = df_1.iloc[-1]['metric']
            metric_string = 'Best {} is {} with {}'.format(human_readable_translation.get(factor, factor),
                                                           human_readable_translation.get(best, best),
                                                           human_readable_translation.get(query_params.sql_metric, query_params.sql_metric))
            metrics[metric_string] = round(metric_for_best, 3)
        else:
            worst = df_1.iloc[0]['factor']
            metric_for_worst = df_1.iloc[0]['metric']
            metric_string = 'Worst {} is {} with {}'.format(human_readable_translation.get(factor),
                                                            human_readable_translation.get(worst, worst),
                                                            human_readable_translation.get(query_params.sql_metric, query_params.sql_metric))
            metrics[metric_string] = round(metric_for_worst, 3)

        # Round decimals to 3 places
        df_1 = df_1.round(3)

        # Filter most important
        df_1 = df_1.iloc[-15:,:]
        df_1 = df_1.reset_index(drop = True)

        # Make plot
        text = 'hello'
        plot1 = visualizations.makeplot('hbar', df_1, query_params, metrics)

    '''
    Upper right chart
    '''
    if query_params.metric == 'netwins' or query_params.intent == 'machine_performance':
        if query_params.ordering != 'date' or query_params.intent == 'machine_performance':
            print df_1
            print len(df_1)
            mainfactors = []
            if len(df_1) <= 15:
                for i in xrange(1, len(df_1) + 1):
                    mainfactors.append((df_1.iloc[-i]['factor'], '', helper.convert_money_to_string(df_1.iloc[-i]['metric'])))
            else:
                for i in xrange(1,16):
                    mainfactors.append((df_1.iloc[-i]['factor'], '', helper.convert_money_to_string(df_1.iloc[-i]['metric'])))
            table_metrics = [('Net Win PUPD')]
        else:
            # Calculate the main factors driving change in metric
            mainfactors_df = factor_analysis.get_main_factors(df)
            mainfactors = factor_analysis.translate_mainfactors_df_into_list(mainfactors_df)
            table_metrics = [('Total Net Win')]
    else:
        table_metrics = None
        mainfactors = None


    '''
    Bottom left plot
    '''
    # Find the top factor from mainfactors
    if query_params.ordering != 'date' or query_params.intent == 'machine_performance':
        plot2 = ''
    else:
        # Make plot 2
        # specific_factor = mainfactors[0][0]
        # if specific_factor[:4] == 'AREA':
        #     factor = 'area'
        # elif specific_factor[:4] == 'BANK':
        #     factor = 'bank'
        # elif specific_factor[:4] == 'ZONE':
        #     factor = 'zone'
        # else:
        #     factor = 'clublevel'
        #
        # df_1 = helper.filter_by_specific_factor(df, factor, specific_factor)
        # print df_1.head()
        # text = 'hello'
        # plot2 = visualizations.makeplot('line', df_1, query_params, text)
        plot2 = ''

    '''
    Bottom right chart
    '''
    derivedmetrics = factor_analysis.create_derivedmetrics()
    # derivedmetrics = None

    return plot1, None, mainfactors, derivedmetrics, None, metrics, table_metrics, None, None, None, None

if __name__ == "__main__":
    query = 'how are my machines doing january'
    query = 'how are my machines doing january'
    query = 'what is my net win'
    main(query, error_checking = False)
