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
import factor_analysis
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
            df_subgroup = df[df[factor] == unique_item]

            # Make plot
            plt.plot(df_subgroup.tmstmp, df_subgroup.metric, label = unique_item)

            # Shade under curve
            axes = plt.gca()
            min_y = axes.get_ylim()[0]
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
    plot1 = create_line_graph(df, query_params, title)
    return plot1

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
    main_factor = factor_analysis.find_factor_of_top_factor(top_factor, factor_df)
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

    # mainfactors_df = get_main_factors(factor_df)
    # mainfactors = translate_mainfactors_df_into_list(mainfactors_df)
    # # df_2, plot2 = create_plot_2(mainfactors, factor_df, query)
    # top_factor = mainfactors[0][0]
    # main_factor = find_factor_of_top_factor(top_factor, factor_df)
    # df_2 = factor_df.groupby(['tmstmp', main_factor]).sum().reset_index()
    # main_factors = pd.unique(df_2[main_factor])
    # fig = plt.figure()
    # plt.show()
    #
    # for factor in main_factors:
    #     subdf = df_2[df_2[main_factor] == factor]
    #     plt.plot(subdf.tmstmp, subdf.metric, label=factor)
    # plt.xlabel('time stamp')
    # plt.ylabel('metric')
    # plt.legend(loc='best')
    # plot2 = mpld3.fig_to_html(fig)
    # derivedmetrics = create_derivedmetrics()
    # plot1, plot2, mainfactors, derivedmetrics = create_visualizations(query)
