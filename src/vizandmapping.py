# -*- coding: utf-8 -*-
# Copyright 2016 IBM Corp. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import helper
import json
import pandas as pd
import matplotlib.pyplot as plt
import mpld3
import numpy as np
import seaborn as sns
import requests
from datetime import timedelta
from sqlalchemy import create_engine
from generateresponsefromrequest import get_intent_entity_from_watson
from rulebasedquery import query_parameters, translation_dictionary

'''
The purpose of this module is to produce four objects through four
major processes:
INPUT:
    single natural language query
OUTPUT:
    plot1 (unicode): this is the html, css, javascript to render the
    mpld3 plot
    mainfactors (list): this is a list of tuples where each element
    is three items - the metric, the direction, and the percent change
    plot2 (unicode): this is the html, css, javascript to render the
    mpld3 plot
    derivedmetrics (list): this is a list of tuples where each element
    is three items - the metric, the direction, and the percent change
Where mainfactors are derived from plot1 trend data, plot2 visualizes the
most important factor in plot2, and then derivedmetrics are the most
influential metric changes over the time period specified
'''

# Local database
# DATABASE_USER = 'test'
# DATABASE_NAME = 'playlogs'
# DATABASE_DOMAIN = 'tiger@localhost'
# DATABASE_TABLE = 'logs'

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

def get_data_from_nl_query(nl_query, error_checking = False):
    '''
    Args:
        nl_query (str): this is a natural language query
        i.e. what is my revenue today
    Returns
        df (dataframe): this is a pandas dataframe that contains a table
        which will be used for visualization
        data (dict): this is a dict needed to interact with the rulebasedapi,
        the key is "query" and the value is the actual natural language query
    '''
    # Get JSON Watson conversations response to natual language query
    response = get_intent_entity_from_watson(nl_query, error_checking = False)

    # Transform JSON Watson conversations response to query parameters object
    query_params = query_parameters()
    query_params.generate_query_params_from_response(nl_query, response, error_checking = False)

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

    # Add main factors
    query_params.sql_factors += main_factors

    # Generate SQL query
    query_params.generate_sql_query(error_checking = True)

    # Get SQL query string from query parameters object
    sql_query = query_params.sql_string
    if error_checking:
        print "SQL query string: {}".format(sql_query)

    # Place SQL results into DataFrame
    df = helper.get_sql_data(sql_query, engine)
    return df, query_params

def create_line_graph(df, query_params, title):
    '''
    Input:
        df (DataFrame) -- DataFrame with data we want to graph
        query_params (query_params object) -- self explanatory
    Output:
        HTML string representation of Matplotlib figure
    '''
    # Create figure object
    fig = plt.figure()
    axes = plt.gca()

    # Check if we are building single or multi-line graph
    if len(df.columns) == 2:
        # Make plot for single-line graph
        plt.plot(df.tmstmp, df.metric)
        plt.xlabel('Time')
        plt.ylabel(query_params.metric)

        # Shade under curve
        min_y = axes.get_ylim()[0]
        plt.fill_between(df.tmstmp.values, df.metric.values, min_y, alpha = 0.5)
    else:
        # Plot multi-line graph
        factor = translation_dictionary.get(query_params.factors[0], query_params.factors[0])
        for unique_item in df[factor].unique():
            print unique_item
            df_subgroup = df[df[factor] == unique_item]

            # Make plot
            plt.plot(df_subgroup.tmstmp, df_subgroup.metric, label = unique_item)

            # Shade under curve
            # axes = plt.gca()
            # min_y = axes.get_ylim()[0]
            # plt.fill_between(df_subgroup.tmstmp.values, df_subgroup.metric.values, min_y, alpha = 0.5)

    # Label axes
    plt.xlabel('Time')
    plt.ylabel(query_params.metric)

    # Add legend
    plt.legend()

    # Add title
    axes.set_title(title)

    # Convert to D3
    fig_d3 = mpld3.fig_to_html(fig)
    return fig_d3

def create_multiline_graph(df, query_params):
    return

def create_plot_1(df, query_params, title):
    '''
    Args:
        df (dataframe): this is the dataframe output of the initial query call
    Returns
        plot1 (unicode): this is the html, css, javascript to render the
        mpld3 plot
    To Do's:
        need to find a way to intelligently identify factors in the initial
        query in order to automatically multiplot these broken down plots
    '''
    # if len(df.columns) == 2:
    #     fig = plt.figure()
    #     plt.plot(df.tmstmp, df.metric)
    #     plt.xlabel('time period')
    #     plt.ylabel('metric')
    #     axes = plt.gca()
    #
    #     min_y = axes.get_ylim()[0]
    #     plt.fill_between(df.tmstmp.values, df.metric.values, min_y, alpha=0.5)
    #     plot1 = mpld3.fig_to_html(fig)
    # else:
    #     main_factor = df.columns[2]
    #     main_factors = pd.unique(df[main_factor])
    #     fig = plt.figure()
    #     for factor in main_factors:
    #         subdf = df[df[main_factor] == factor]
    #         plt.fill_between(subdf.tmstmp.values, subdf.metric.values, alpha=0.5)
    #         plt.plot(subdf.tmstmp, subdf.metric, label=factor)
    #     plt.xlabel('time stamp')
    #     plt.ylabel('metric')
    #     plt.legend(loc='best')
    #     plot1 = mpld3.fig_to_html(fig)
    plot1 = create_line_graph(df, query_params, title)
    return plot1


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


def create_plot_2(mainfactors, factor_df, nl_query):
    '''
    Args:
        mainfactors (list): this is a list of tuples where each element is made
        of three parts (factor, direction, absolute difference)
        factor_df (dataframe): this is the culled dataframe which has the
        expanded factors added to it
        nl_query (str): this is a natural language query
        i.e. what is my revenue today
    Returns:
        df_2 (dataframe): this is a pandas dataframe that contains a table
        which will be used for visualization
        data_2 (dict): this is a dict needed to interact with the rulebasedapi,
        the key is "query" and the value is the actual natural language query
        plot2 (unicode): this is the html, css, javascript to render the
        mpld3 plot
    '''
    top_factor = mainfactors[0][0]
    main_factor = find_factor_of_top_factor(top_factor, factor_df)
    df_2 = factor_df.groupby(['tmstmp', main_factor]).sum().reset_index()
    main_factors = pd.unique(df_2[main_factor])
    fig = plt.figure()
    for factor in main_factors:
        subdf = df_2[df_2[main_factor] == factor]
        plt.plot(subdf.tmstmp, subdf.metric, label=factor)
    plt.xlabel('time stamp')
    plt.ylabel('metric')
    plt.legend(loc='best')
    plot2 = mpld3.fig_to_html(fig)
    return df_2, plot2


def identify_derived_metrics():
    '''
    Args:
    Returns:
        derivedmetrics (list): this is a list of tuples where each element
        is three items - the metric, the direction, and the percent change
    '''


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


def create_visualizations(query):
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
    '''
    # Pull down data from database
    df, query_params = get_data_from_nl_query(query)

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
    if query_params.ordering == 'date':
        # Ordering is by date, so show line graph
        if query_params.period:
            # No need to provide single value metric
            title = 'Time metric'
            df_1 = helper.sum_by_time(df, None)
            plot1 = create_plot_1(df_1, query_params, title)
            if query_params.factors:
                # Multi-line graph
                title = 'Time metric'
                factor = translation_dictionary.get(query_params.factors[0], query_params.factors[0])
                df_1 = helper.sum_by_time(df, factor)
                plot1 = create_plot_1(df_1, query_params, title)
            else:
                # Single line graph
                df_1 = helper.sum_by_time(df, None)
                plot1 = create_plot_1(df_1, query_params, title)
        else:
            if query_params.factors:
                # Multi-line graph
                title = 'Time metric'
                factor = translation_dictionary.get(query_params.factors[0], query_params.factors[0])
                df_1 = helper.sum_by_time(df, factor)
                plot1 = create_plot_1(df_1, query_params, title)
            else:
                # Single line graph
                df_1 = helper.sum_by_time(df, None)
                plot1 = create_plot_1(df_1, query_params, title)
            df_1 = helper.sum_by_time(df, None)
            print df_1.head()

            # Calculate metric total we are interest in
            total = df_1['metric'].sum()
            title = 'Total {} from {} to {} is {}'.format(query_params.metric,
                                                          query_params.start,
                                                          query_params.stop,
                                                          total)
            plot1 = create_plot_1(df_1, query_params, title)
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

    mainfactors_df = get_main_factors(df)
    mainfactors = translate_mainfactors_df_into_list(mainfactors_df)
    df_2, plot2 = create_plot_2(mainfactors, df, query)
    derivedmetrics = create_derivedmetrics()
    return plot1, plot2, mainfactors[:15], derivedmetrics

if __name__ == "__main__":
    query = 'what is my revenue january 1st 2015 by area'
    plot1, plot2, mainfactors, derivedmetrics = create_visualizations(query)

    # plt.show()

    # query = query_params.query + \
    #     ', game title, manufacturer, zone, bank, stand, wager, club level'
    # query = query_params.query + \
    #     ', game title, club level'

    # Get JSON Watson conversations response to natual language query
    # response = get_intent_entity_from_watson(query)

    # Transform JSON Watson conversations response to query parameters object
    # query_params = query_parameters()
    # query_params.generate_query_params_from_response(nl_query, response, error_checking = True)

    query_params.generate_sql_query()

    print query_params

    # Get SQL query string from query parameters object
    sql_query = query_params.sql_string

    # Place SQL results into DataFrame
    factor_df = helper.get_sql_data(sql_query, engine)

    print factor_df.head()
    raw_input()

    mainfactors_df = get_main_factors(factor_df)
    mainfactors = translate_mainfactors_df_into_list(mainfactors_df)
    # df_2, plot2 = create_plot_2(mainfactors, factor_df, query)
    top_factor = mainfactors[0][0]
    main_factor = find_factor_of_top_factor(top_factor, factor_df)
    df_2 = factor_df.groupby(['tmstmp', main_factor]).sum().reset_index()
    main_factors = pd.unique(df_2[main_factor])
    fig = plt.figure()
    plt.show()

    for factor in main_factors:
        subdf = df_2[df_2[main_factor] == factor]
        plt.plot(subdf.tmstmp, subdf.metric, label=factor)
    plt.xlabel('time stamp')
    plt.ylabel('metric')
    plt.legend(loc='best')
    plot2 = mpld3.fig_to_html(fig)
    derivedmetrics = create_derivedmetrics()
    # plot1, plot2, mainfactors, derivedmetrics = create_visualizations(query)
