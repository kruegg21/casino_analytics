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

from generateresponsefromrequest import get_intent_entity_from_watson
from datetime import datetime
import json
import pandas as pd
import psycopg2
from pytz import timezone
import helper

TIME_ZONE = timezone('US/Pacific')
DATABASE_USER = 'test'
DATABASE_NAME = 'playlogs'
DATABASE_DOMAIN = 'tiger@localhost'
DATABASE_TABLE = 'logs'

'''
The purpose of this module is to take care of the default parameters
that should be set fo the different arguments that are going to get
passed into the data service query.

Set the defauls in the __init__ function of the query parameters class.
'''


translation_dictionary = {'revenue': 'SUM(amountbet - amountwon)',
                          'popularity': 'COUNT(*)',
                          'games played': 'SUM(gamesplayed)',
                          'payout rate': 'SUM(amountwon) / SUM(amountbet)',
                          'club level': 'clublevel',
                          'game title': 'assettitle',
                          'wager': 'denom',
                          'daily': 'day',
                          'weekly': 'week',
                          'monthly': 'month',
                          'yearly': 'year',
                          'by minute': 'minute',
                          'hourly': 'hour',
                          'best': 'ORDER BY 1 DESC',
                          'worst': 'ORDER BY 1 ASC',
                          'date': 'ORDER BY 2',
                          'bronze': 'BRONZE',
                          'silver': 'SILVER',
                          'platinum': 'PLATINUM',
                          'gold': 'GOLD',
                          'average': 'AVG',
                          'median': 'MIN'}


class query_parameters(object):
    def __init__(self):
        self.query = 'Nothing here'
        self.intent = 'metric_by_factor_by_time_period'
        self.intent_confidence = 0.0
        self.metric = None
        self.factors = []
        self.start = datetime.strptime('2015-01-01', '%Y-%m-%d')
        self.stop = datetime.strptime('2015-01-02', '%Y-%m-%d')
        self.period = None
        self.ordering = 'date'
        self.club_level = None
        self.statistic = None
        self.sql_string = None

    def __str__(self):
        return "Given query: {}\n".format(self.query) + \
               "Intent: {} with confidence of {}\n".format(self.intent, self.intent_confidence) + \
               "Factor: {}\n".format(self.factors) + \
               "Period: {}\n".format(self.period) + \
               "Metric: {}\n".format(self.metric) + \
               "Start: {}\n".format(self.start) + \
               "Stop: {}\n".format(self.stop) + \
               "Ordering: {}\n".format(self.ordering) + \
               "Club Level: {}\n".format(self.club_level) + \
               "Statistic: {}\n".format(self.statistic) + \
               "SQL Query: {}\n".format(self.sql_string)

    def translate_to_sql(self):
        if not self.period:
            self.period = 'daily'

        if not self.metric:
            self.metric = 'revenue'

        self.sql_metric = translation_dictionary.get(self.metric, self.metric)
        self.sql_factors = [translation_dictionary.get(
            x, x) for x in self.factors]
        self.sql_period = translation_dictionary.get(self.period, self.period)
        self.sql_ordering = translation_dictionary.get(
            self.ordering, self.ordering)
        self.sql_start = self.start.strftime("%Y-%m-%d-00-00-00-000")
        self.sql_stop = self.stop.strftime("%Y-%m-%d-23-59-59-999")

        if self.club_level:
            self.sql_club_level = """AND clublevel = '{}'""".format(
                translation_dictionary.get(self.club_level, self.club_level))
        else:
            self.sql_club_level = ''

        if self.statistic:
            self.sql_statistic = """SELECT {}(metric) FROM (""".format(
                translation_dictionary.get(self.statistic, self.statistic))
        else:
            self.sql_statistic = ''


@helper.timeit
def get_query_params_from_response(response, query, error_checking=False):
    '''
    Input:
        response (JSON): this is the raw JSON response from Watson chatbot API
        query (str): this is the string of the original query
    Output:
        query_parameters object populated with parameters from chatbot API
        including the SQL query
    '''

    # Turn JSON into Python dictionary
    response_dict = json.loads(json.dumps(response))['intent_entity_mapping']

    # Create query parameters object
    query_params = query_parameters()

    # Add query to query parameters object
    query_params.query = query

    # Look at intent
    query_params.intent = response_dict['intents'][0]['intent']
    query_params.intent_confidence = response_dict['intents'][0]['confidence']

    # Look through possible entities
    entities_list = response_dict['entities']

    date_list = []
    factor_list = []
    for entity in entities_list:
        if error_checking:
            print entity
        if entity['entity'] == 'player_factors':
            factor_list.append(entity['value'])
        if entity['entity'] == 'machine_factors':
            factor_list.append(entity['value'])
        if entity['entity'] == 'time_period':
            query_params.period = entity['value']
        if entity['entity'] == 'both_metrics':
            query_params.metric = entity['value']
        if entity['entity'] == 'top':
            query_params.ordering = entity['value']
        if entity['entity'] == 'club_level':
            query_params.club_level = entity['value']
        if entity['entity'] == 'statistics':
            query_params.statistic = entity['value']
        if entity['entity'] == 'sys-date':
            year = int(entity['value'][:4])
            month = int(entity['value'][5:7])
            day = int(entity['value'][8:])
            date = datetime(year, month, day, tzinfo=TIME_ZONE)
            date_list.append(date)

    # Add factor list to query parameters
    query_params.factors = list(set(factor_list))

    # Find start and stop from date entities
    if date_list:
        query_params.start = min(date_list)
        query_params.stop = max(date_list)

    # Translate entities to SQL
    query_params.translate_to_sql()

    # Create string formatting
    if query_params.sql_statistic:
        suffix = ') AS t'
    else:
        suffix = ''

    factors_string = ''
    additional_group_by = ''
    ctr = 0
    for factor in query_params.sql_factors:
        factors_string += ', '
        factors_string += factor
        additional_group_by += ', ' + str(3 + ctr)
        ctr += 1

    # Execute SQL query
    SQL_string = \
        """{}SELECT {} AS metric, date_trunc('{}', {}.tmstmp) AS tmstmp{}
           FROM {}
           WHERE tmstmp >= to_timestamp('{}', 'YYYY-MM-DD-HH24-MI-SS-MS')
           AND tmstmp <= to_timestamp('{}', 'YYYY-MM-DD-HH24-MI-SS-MS') {}
           GROUP BY 2{}
           {}{}""".format(query_params.sql_statistic,
                          query_params.sql_metric,
                          query_params.sql_period,
                          DATABASE_TABLE,
                          factors_string,
                          DATABASE_TABLE,
                          query_params.sql_start,
                          query_params.sql_stop,
                          query_params.sql_club_level,
                          additional_group_by,
                          query_params.sql_ordering,
                          suffix)

    # SQL_string = \
    #     """{}SELECT *
    #        FROM {}_{}
    #        WHERE {} >= to_timestamp('{}', 'YYYY-MM-DD-HH24-MI-SS-MS')
    #        AND {} <= to_timestamp('{}', 'YYYY-MM-DD-HH24-MI-SS-MS')""".format(query_params.sql_statistic,
    #                       query_params.metric,
    #                       query_params.period,
    #                       query_params.sql_period,
    #                       query_params.sql_start,
    #                       query_params.sql_period,
    #                       query_params.sql_stop)

    query_params.sql_string = SQL_string
    return query_params


if __name__ == "__main__":
    # query = 'games played by area january 1st 2015?'
    query = 'what is my hourly revenue by club level, area, zone, stand, wager, manufacturer, game title'
    query = 'what is the payout rate for january 2 2015'
    query = 'hourly revenue by club level, area, zone, stand, wager'
    response = get_intent_entity_from_watson(query)

    query_params = get_query_params_from_response(response, query)
    engine = helper.connect_to_database(
        DATABASE_USER, DATABASE_DOMAIN, DATABASE_NAME)
    df = helper.get_sql_data(query_params.sql_string, engine)
    print df.head(2000)
