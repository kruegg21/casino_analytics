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
import matplotlib.pyplot as plt
import mpld3
import numpy as np
import seaborn
from translation_dictionaries import *

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
    INPUT: string, pandas dataframe, object
    OUTPUT: plot as html string

    Takes in a string for a type of plot and and a dataframe and makes an html
    string for a plot of that dataframe
    '''

    plot = {"line": line_plot, "hbar": hbar_plot}

    return mpld3.fig_to_html(plot[p_type](df, query_params))


def line_plot(df, query_params):
    '''
    INPUT: pandas dataframe
    OUTPUT: matplotlib figure
    '''

    fig, ax = plt.subplots()

    if df.shape[1] == 2:
        # Make plot for single-line graph
        plt.plot(df.tmstmp, df.metric)
        plt.xlabel('Time')
        plt.ylabel(human_readable_translation[query_params.sql_metric])

        # Shade under curve
        min_y = ax.get_ylim()[0]
        plt.fill_between(df.tmstmp.values, df.metric.values, min_y, alpha=0.5)
    elif df.shape[0] > 2:
        # Plot multi-line graph
        for unique_item in df['factor'].unique():
            df_subgroup = df[df['factor'] == unique_item]
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


def hbar_plot(df, query_params):
    """
    INPUT: dataframe with metric and factor columns
    OUTPUT: matplotlib figure

    Returns a horizontal bar plot
    """
    fig, ax = plt.subplots()

    # Add label for x-axis
    plt.xlabel(human_readable_translation[query_params.sql_metric])

    y_pos = range(df.shape[0])
    ax.barh(y_pos, df.metric, align="center", tick_label=df.factor)
    for i, bar in enumerate(ax.get_children()):
        tooltip = mpld3.plugins.LineLabelTooltip(bar, label=fig.factor[i])
        mpld3.plugins.connect(fig, tooltip)
    return fig


def hist_plot(df, query_params):
    """
    INPUT: dataframe with metric and factor columns
    OUTPUT: matplotlib figure
    """
    fig, ax = plt.subplots()
    bins = np.floor(
        min(max(20, np.sqrt(df.metric.max() - df.metric.min())), 50))
    ax.hist(df.metric, bins=bins)
    return fig


if __name__ == "__main__":
    pass
