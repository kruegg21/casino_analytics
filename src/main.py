import helper
import json
import pandas as pd
import mpld3
import numpy as np
import requests
import factor_analysis
import vizandmapping
from datetime import timedelta
from sqlalchemy import create_engine
from generateresponsefromrequest import get_intent_entity_from_watson
from rulebasedquery import query_parameters, translation_dictionary

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

def impute_period(query_params, error_checking = False):
    # Check to see if a period is specified, if not impute period based on range
    if not query_params.period:
        period = None
        time_range = query_params.stop - query_params.start
        if time_range > timedelta(hours = 23, minutes = 59, seconds = 59):
            # Range is greater than a day
            print "Range is greater than a day" if error_checking else None
            if time_range > timedelta(days = 6, hours = 23, minutes = 59, seconds = 59):
                # Range is greater than a week
                print "Range is greater than a week" if error_checking else None
                if time_range > timedelta(days = 27, hours = 23, minutes = 59, seconds = 59):
                    # Range is greater than a month
                    print "Range is greater than a month" if error_checking else None
                    if time_range > timedelta(days = 364, hours = 23, minutes = 59, seconds = 59):
                        # Range is greater than a year
                        print "Range is greater than a year" if error_checking else None
                        # Segment by months
                        period = 'monthly'
                    else:
                        # Range is less than a year
                        print "Range is less than a year" if error_checking else None
                        # Segment by weeks
                        period = 'weekly'
                else:
                    # Range is less than a month
                    print "Range is less than a month" if error_checking else None
                    # Segment by days
                    period = 'daily'
            else:
                # Range is less than week
                print "Range is less than a week" if error_checking else None
                # Segment by hour
                period = 'hourly'
        else:
            print "Range is smaller than a day" if error_checking else None
            # Segment by minute
            period = 'by_minute'

        # Add imputed period
        query_params.sql_period = translation_dictionary[period]
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
    query_params.generate_query_params_from_response(nl_query, response, error_checking = False)

    # Add main factors
    query_params.sql_factors += main_factors

    # Impute period if needed
    query_params = impute_period(query_params)

    # Generate SQL query
    query_params.generate_sql_query(error_checking = True)

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
    plot1 = None
    print query_params

    # Dictionary to hold calculated metrics
    metrics = {}

    # Determine metrics and graph type to build
    if query_params.ordering == 'date':
        # Line graph

        # Currently supports only single factor
        if query_params.factors:
            factor = translation_dictionary.get(query_params.factors[0], query_params.factors[0])
        else:
            factor = None
        title = 'Line graph'
        print df.head()
        df_1 = helper.sum_by_time(df, factor)
        print df_1.head()
        raw_input()
        plot1 = vizandmapping.create_plot_1(df_1, query_params, title)

        # Calculate metric total we are interest in
        total = df_1['metric'].sum()
        title = 'Total {} from {} to {} is {}'.format(query_params.metric,
                                                      query_params.start,
                                                      query_params.stop,
                                                      total)

        # # Ordering is by date, so show line graph
        # if query_params.period:
        #     # No need to provide single value metric
        #     title = 'Time metric'
        #     df_1 = helper.sum_by_time(df, None)
        #     plot1 = create_plot_1(df_1, query_params, title)
        #     if query_params.factors:
        #         # Multi-line graph
        #         title = 'Time metric'
        #         factor = translation_dictionary.get(query_params.factors[0], query_params.factors[0])
        #         df_1 = helper.sum_by_time(df, factor)
        #         plot1 = create_plot_1(df_1, query_params, title)
        #     else:
        #         # Single line graph
        #         df_1 = helper.sum_by_time(df, None)
        #         plot1 = create_plot_1(df_1, query_params, title)
        # else:
        #     if query_params.factors:
        #         # Multi-line graph
        #         title = 'Time metric'
        #         factor = translation_dictionary.get(query_params.factors[0], query_params.factors[0])
        #         df_1 = helper.sum_by_time(df, factor)
        #         plot1 = create_plot_1(df_1, query_params, title)
        #     else:
        #         # Single line graph
        #         df_1 = helper.sum_by_time(df, None)
        #         plot1 = create_plot_1(df_1, query_params, title)
        #     df_1 = helper.sum_by_time(df, None)
        #     print df_1.head()

    #     if query_params.period:
    #         if query_params.factors:
    #             if query_params.club_level or query_params.area or \
    #                query_params.game_title or query_params.manufacturer or \
    #                query_params.stand or query_params.zone or query_params.bank:
    #                # What is the daily revenue for males by club level?
    #                # Get data divided by important factors and aggregate / filter
    #                # Multi-line graph
    #                pass
    #             else:
    #                 # What is the daily revenue by club level?
    #                 # Get data divided by important factors and aggregate
    #                 # Multi-line graph
    #                 pass
    #
    #             # What is revenue by club level January?
    #             # Get data divided by important factors and aggregate
    #             # Multi-line graph
    #         # What is the revenue for January daily?
    #         # Get data divided by important factors and aggregate
    #     else:
    #         # What is revenue today?
    #         # Infer best segmented data
    #         # Possible segments are by minute, hourly, daily, weekly, monthly, yearly
    #         pass
    else:
        # Histogram deal with this later
        pass

    mainfactors_df = factor_analysis.get_main_factors(df)
    mainfactors = factor_analysis.translate_mainfactors_df_into_list(mainfactors_df)
    df_2, plot2 = vizandmapping.create_plot_2(mainfactors, df, query)
    derivedmetrics = factor_analysis.create_derivedmetrics()
    return plot1, plot2, mainfactors[:15], derivedmetrics

if __name__ == "__main__":
    query = 'what is my revenue weekly for january 2015'
    main(query, error_checking = True)
