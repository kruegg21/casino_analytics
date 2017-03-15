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

import pandas as pd
import matplotlib.pyplot as plt
import mpld3
import numpy as np
import seaborn as sns
from sqlalchemy import create_engine
import requests


engine = \
    create_engine('postgres://amzpgbpy:OpBSqqC5OxuTuF8Iss09lSzJkX_PlZMf@soft-pomegranate.db.elephantsql.com:5432/amzpgbpy')


def create_plot_1():
    '''
    Args:
        None
    Returns:
        plot1 (unicode): this is the html, css, javascript to render the
        mpld3 plot
    '''
    fig = plt.figure()
    x = range(100)
    y = np.random.randint(1, 100, 100)
    plt.plot(x, y, label='plot1')
    plot1 = mpld3.fig_to_html(fig)
    return plot1


def create_plot_2():
    '''
    Args:
        None
    Returns:
        plot2 (unicode): this is the html, css, javascript to render the
        mpld3 plot
    '''
    fig = plt.figure()
    x = range(100)
    y = np.random.randint(1, 100, 100)
    plt.plot(x, y, label='plot2')
    plt.figure()
    plot2 = mpld3.fig_to_html(fig)
    return plot2


def create_mainfactors():
    '''
    Args:
        None
    Returns:
        mainfactors (list): this is a list of tuples where each element
        is three items - the metric, the direction, and the percent change
    '''
    factors = ['male', 'female', 'gold', 'silver', 'bronze']
    percent = np.random.rand(5) * np.random.choice([1, -1], 5)
    direction = ['up' if pct > 0 else 'down' for pct in percent]
    mainfactors = zip(factors, direction, percent)
    return mainfactors


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


def create_sample_objects():
    '''
    Args:
        None
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
    plot1 = create_plot_1()
    plot2 = create_plot_2()
    mainfactors = create_mainfactors()
    derivedmetrics = create_derivedmetrics()
    return plot1, plot2, mainfactors, derivedmetrics


if __name__ == "__main__":
    query = 'what is my revenue'
    link = 'https://gaminganalytics-rulebasedapi-host.mybluemix.net/sqlquery'
    data = {'query': query}
    response = requests.post(link, json=data)
    sql_query = response.content
    df = pd.read_sql_query(sql_query, con=engine)
    print(df.info())
