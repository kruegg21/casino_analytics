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

# from sqlalchemy import create_engine
# import json
from mpld3 import plugins
from rulebasedquery import translation_dictionary
import factor_analysis
import matplotlib.pyplot as plt
import mpld3
import numpy as np
import pandas as pd


# # Read password from external file
# with open('passwords.json') as data_file:
#     data = json.load(data_file)
#
# DATABASE_HOST = 'soft-feijoa.db.elephantsql.com'
# DATABASE_PORT = '5432'
# DATABASE_NAME = 'ohdimqey'
# DATABASE_USER = 'ohdimqey'
# DATABASE_PASSWORD = data['DATABASE_PASSWORD']
#
# # Connect to database
# database_string = 'postgres://{}:{}@{}:{}/{}'.format(DATABASE_USER,
#                                                      DATABASE_PASSWORD,
#                                                      DATABASE_HOST,
#                                                      DATABASE_PORT,
#                                                      DATABASE_NAME)
# engine = create_engine(database_string)

def makeplot(p_type, df, query_params):
    '''
    INPUT: string, pandas dataframe
    OUTPUT: plot as html string

    Takes in a string for a type of plot and and a dataframe and makes an html
    string for a plot of that dataframe
    '''

    plot = {"line": line_plot}

    if df.shape[0] > 1:
        return mpld3.fig_to_html(plot[p_type](df, query_params))
    else:
        raise ValueError("Passed Dataframe needs at least two columns")


def line_plot(df, query_params):
    '''
    INPUT: pandas dataframe
    OUTPUT: matplotlib figure
    '''

    fig, ax = plt.subplots()

    if df.shape[0] == 2:
        # Make plot for single-line graph
        plt.plot(df.tmstmp, df.metric)
        plt.xlabel('Time')
        plt.ylabel(query_params.metric)

        # Shade under curve
        min_y = ax.get_ylim()[0]
        plt.fill_between(df.tmstmp.values, df.metric.values, min_y, alpha=0.5)
    elif df.shape[0] > 2:
        # Plot multi-line graph
        factor = translation_dictionary.get(
            query_params.factors[0], query_params.factors[0])

        for unique_item in df[factor].unique():
            df_subgroup = df[df[factor] == unique_item]
            # Make plot
            plt.plot(df_subgroup.tmstmp, df_subgroup.metric, label=unique_item)
            # Make interactive legend
            handles, labels = ax.get_legend_handles_labels()
            interactive_legend = plugins.InteractiveLegendPlugin(zip(
                handles, ax.collections), labels, alpha_unsel=0.5, alpha_over=1.5, start_visible=True)
            plugins.connect(fig, interactive_legend)
    else:
        pass

    return fig


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
        plt.fill_between(df.tmstmp.values, df.metric.values, min_y, alpha=0.5)
    else:
        # Plot multi-line graph
        factor = translation_dictionary.get(
            query_params.factors[0], query_params.factors[0])

        for unique_item in df[factor].unique():
            df_subgroup = df[df[factor] == unique_item]

            # Make plot
            plt.plot(df_subgroup.tmstmp, df_subgroup.metric, label=unique_item)

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
    main_factor = factor_analysis.find_factor_of_top_factor(
        top_factor, factor_df)
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
    pass
