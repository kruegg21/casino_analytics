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
        '''
        Top left plot
        '''
        # Create quadrant object
        tl_quadrant = quadrant(viz_type = 'plot')

        # Group by time period to get totals of metric
        #   tmstmp        metric
        # 0 2015-04-01  1.776029e+06
        # 1 2015-05-01  1.807870e+06
        # 2 2015-06-01  1.766107e+06
        # 3 2015-07-01  1.845381e+06
        # 4 2015-08-01  1.870472e+06
        df_total = helper.sum_by_time(df, None)
        total = helper.convert_money_to_string(df_total.iloc[-1].metric)

        # Convert metric to PUPD adjucted metric
        #   tmstmp      metric
        # 0 2015-04-01  298.391909
        # 1 2015-05-01  303.741582
        # 2 2015-06-01  296.724972
        # 3 2015-07-01  310.043796
        # 4 2015-08-01  314.259388
        df_pupd = helper.calculate_pupd(df_total, query_params)
        pupd = helper.convert_money_to_string(df_pupd.iloc[-1].metric)

        # Add total and PUPD text to metrics dictionary
        readable_period = human_readable_translation[query_params.period]
        metrics['Number of Machines'] = query_params.num_machines
        metrics['Total Net Win for this {}'.format(readable_period)] = total
        metrics['Net Win PUPD for this {}'.format(readable_period)] = pupd

        # Make plot
        tl_quadrant.plot = visualizations.makeplot('line', df_pupd, query_params, metrics)

        '''
        Top right table
        '''
        # Create quadrant object
        tr_quadrant = quadrant(viz_type = 'table')

        # Make column titles
        column_titles = [(readable_period),
                         ('Net Win PUPD')]
        title = ('Net Win PUPD by {}'.format(readable_period),)

        # Create table data
        table_data = []
        for i in xrange(1, len(df_pupd) + 1):
            tmstmp = helper.convert_datetime_to_string(df_pupd.iloc[-i]['tmstmp'], query_params.sql_period)
            metric = helper.convert_money_to_string(df_pupd.iloc[-i]['metric'])
            table_data.append((tmstmp, metric))
        tr_quadrant.title = title
        tr_quadrant.column_titles = column_titles
        tr_quadrant.table_data = table_data

        '''
        Bottom left table:
        Net win by bank analysis. There is opportunity to speed this up by only
        SQL querying data from the most recent period.
        '''
        factor = 'bank'
        machines_per_bank = 4

        # Create quadrant object
        bl_quadrant = quadrant(viz_type = 'table')

        # Generate new query params object
        query_params_bl = query_parameters()
        query_params_bl.start = query_params.start
        query_params_bl.stop = query_params.stop
        query_params_bl.period = query_params.period
        query_params_bl.factors = main_factors

        # Generate new SQL query
        query_params_bl.generate_sql_query()

        # Pull data down
        df = helper.get_sql_data(query_params_bl.sql_string, engine)

        # Get data from most recent period
        most_recent_period = df.iloc[-1].tmstmp
        df_recent = df[df.tmstmp == most_recent_period]

        # Group and sum by time period and factor
        #   tmstmp   factor       metric
        # 0 2017-03-01   BANK-1  121500.8000
        # 1 2017-03-01  BANK-10    1987.4550
        # 2 2017-03-01  BANK-11  103462.5950
        # 3 2017-03-01  BANK-12   58532.7836
        # 4 2017-03-01  BANK-13    4754.6033
        df_total = helper.sum_by_time(df_recent, factor)

        # Calculate PUPD adjusting for fact we have 4 machines per bank
        df_pupd = helper.calculate_pupd(df_total, query_params_bl)
        df_pupd.metric = df_pupd.metric * (float(192) / machines_per_bank)

        # Find House Average
        house_average = helper.convert_money_to_string(df_pupd.metric.mean())

        # Sort by Net Win PUPD
        df_pupd.sort_values('metric', inplace = True)

        # Create table
        title = ('Net Win by Bank this {}'.format(readable_period),)
        column_titles = ('Bank', 'Total Net Win', 'Net Win PUPD', 'House Average')
        table_data = []
        for i in xrange(1, len(df_pupd) + 1):
            table_data.append((df_pupd.iloc[-i]['factor'],
                               helper.convert_money_to_string(df_pupd.iloc[-i]['total']),
                               helper.convert_money_to_string(df_pupd.iloc[-i]['metric']),
                               house_average))
        bl_quadrant.title = title
        bl_quadrant.column_titles = column_titles
        bl_quadrant.table_data = table_data

        '''
        Bottom right table
        '''
        # Create quadrant object
        br_quadrant = quadrant(viz_type = 'table')

        # Create new query params object
        query_params_br = query_parameters()
        query_params_br.start = query_params.start
        query_params_br.stop = query_params.stop
        query_params_br.period = query_params.period
        query_params_br.factors = ['assetnumber', 'assettitle']

        # Generate SQL query
        query_params_br.generate_sql_query(error_checking = error_checking)

        # Pull data down
        df = helper.get_sql_data(query_params_br.sql_string, engine)

        # Select only current month data
        current_month = df.iloc[-1].tmstmp
        df_current = df[df.tmstmp == current_month].sort_values('metric')

        # Calculate PUPD
        # metric   tmstmp      assetnumber assettitle                     total
        # 2.884023 2017-03-01  AST-000107  Sweet 3                        89.4047
        # 2.937384 2017-03-01  AST-000174  Michael Jackson Icon           91.0589
        # 3.034935 2017-03-01  AST-000096  Wheel Of Fortune               94.0830
        # 3.081771 2017-03-01  AST-000124  Lucky Larry's Lobstermania     95.5349
        # 3.125777 2017-03-01  AST-000170  Playboy Dont Stop The Party    96.8991
        df_pupd = helper.calculate_pupd(df_current, query_params)
        df_pupd['metric'] = df_pupd.metric * 192

        # Get column titles
        column_titles = ('Machine Number', 'Title', 'Best/Worst', 'Net Win PUPD')
        title = ('Best and Worst Machines for {}'.format(readable_period),)

        # Get table data
        table_data = []
        for i in xrange(1,6):
            row = df_pupd.iloc[-i]
            table_data.append((row.assetnumber, row.assettitle, 'Best', helper.convert_money_to_string(row.metric)))
        for i in xrange(5):
            row = df_pupd.iloc[i]
            table_data.append((row.assetnumber, row.assettitle, 'Worst', helper.convert_money_to_string(row.metric)))
        br_quadrant.column_titles = column_titles
        br_quadrant.title = title
        br_quadrant.table_data = table_data

        return tl_quadrant, tr_quadrant, bl_quadrant, br_quadrant

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
