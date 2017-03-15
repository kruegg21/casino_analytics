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
import pandas as pd
import matplotlib.pyplot as plt
import mpld3
import numpy as np
import seaborn as sns
import requests
from generateresponsefromrequest import get_intent_entity_from_watson
from rulebasedquery import get_query_params_from_response

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
DATABASE_USER = 'test'
DATABASE_NAME = 'playlogs'
DATABASE_DOMAIN = 'tiger@localhost'
DATABASE_TABLE = 'logs'

# Bluemix hosted database
# DATABASE_USER = 'amzpgbpy'
# DATABASE_NAME = 'amzpgbpy'
# DATABASE_DOMAIN = 'OpBSqqC5OxuTuF8Iss09lSzJkX_PlZMf@soft-pomegranate.db.elephantsql.com:5432'
# DATABASE_TABLE = 'logs'

engine = helper.connect_to_database(DATABASE_USER,
                                    DATABASE_DOMAIN,
                                    DATABASE_NAME)


def get_data_from_nl_query(nl_query):
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
    response = get_intent_entity_from_watson(nl_query, error_checking=True)

    # Transform JSON Watson conversations response to query parameters object
    query_params = get_query_params_from_response(response, nl_query)

    # Get SQL query string from query parameters object
    sql_query = query_params.sql_string

    # Place SQL results into DataFrame
    df = helper.get_sql_data(sql_query, engine)
    return df, query_params


def create_plot_1(df):
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
    if len(df.columns) == 2:
        fig = plt.figure()
        plt.plot(df.tmstmp, df.metric)
        plt.xlabel('time period')
        plt.ylabel('metric')
        plt.fill_between(df.tmstmp.values, df.metric.values, alpha=0.5)
        plot1 = mpld3.fig_to_html(fig)
    else:
        main_factor = df.columns[2]
        main_factors = pd.unique(df[main_factor])
        fig = plt.figure()
        for factor in main_factors:
            subdf = df[df[main_factor] == factor]
            plt.plot(subdf.tmstmp, subdf.metric, label=factor)
        plt.fill_between(subdf.tmstmp.values, subdf.metric.values, alpha=0.5)
        plt.xlabel('time stamp')
        plt.ylabel('metric')
        plt.legend(loc='best')
        plot1 = mpld3.fig_to_html(fig)
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
    df, query_params = get_data_from_nl_query(query)
    plot1 = create_plot_1(df)
    query = query_params.query + \
        ', game title, manufacturer, zone, bank, stand, wager, club level'
    factor_df, query_params = get_data_from_nl_query(query)
    mainfactors_df = get_main_factors(factor_df)
    mainfactors = translate_mainfactors_df_into_list(mainfactors_df)
    df_2, plot2 = create_plot_2(mainfactors, factor_df, query)
    derivedmetrics = create_derivedmetrics()
    return plot1, plot2, mainfactors[:15], derivedmetrics


if __name__ == "__main__":
    query = 'what is my daily revenue by club level'
    df, query_params = get_data_from_nl_query(query)

    print df.info()

    plot1 = create_plot_1(df)
    plt.show()
    # query = query_params.query + \
    #     ', game title, manufacturer, zone, bank, stand, wager, club level'
    # factor_df, query_params = get_data_from_nl_query(query)
    #
    #
    # mainfactors_df = get_main_factors(factor_df)
    # mainfactors = translate_mainfactors_df_into_list(mainfactors_df)
    # # df_2, plot2 = create_plot_2(mainfactors, factor_df, query)
    # top_factor = mainfactors[0][0]
    # main_factor = find_factor_of_top_factor(top_factor, factor_df)
    # df_2 = factor_df.groupby(['tmstmp', main_factor]).sum().reset_index()
    # main_factors = pd.unique(df_2[main_factor])
    # fig = plt.figure()
    # plt.show()

    # for factor in main_factors:
    #     subdf = df_2[df_2[main_factor] == factor]
    #     plt.plot(subdf.tmstmp, subdf.metric, label=factor)
    # plt.xlabel('time stamp')
    # plt.ylabel('metric')
    # plt.legend(loc='best')
    # plot2 = mpld3.fig_to_html(fig)
    # derivedmetrics = create_derivedmetrics()
    # plot1, plot2, mainfactors, derivedmetrics = create_visualizations(query)
