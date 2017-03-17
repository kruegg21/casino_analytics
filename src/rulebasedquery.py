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

from datetime import datetime, timedelta
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

from generateresponsefromrequest import get_intent_entity_from_watson

translation_dictionary = {'net' : None,
                          'revenue': 'SUM(amountbet - amountwon)',
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
                          'by_minute': 'minute',
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

# Default metrics and time ranges
DEFAULT_METRIC = 'revenue'
DEFAULT_START = '2015-01-01'
DEFAULT_STOP = '2015-01-31'

class query_parameters(object):
    def __init__(self):
        self.query = 'Nothing here'
        self.intent = 'metric_by_factor_by_time_period'
        self.intent_confidence = 0.0

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

        # SQL string
        self.sql_string = None

        # SQL parameters
        self.sql_metric = None
        self.sql_factors = []
        self.sql_period = None
        self.sql_ordering = None
        self.sql_start = None
        self.sql_stop = None

    def __str__(self):
        return "Given query: {}\n".format(self.query) + \
               "Intent: {} with confidence of {}\n".format(self.intent, self.intent_confidence) + \
               "Factors: {}\n".format(self.factors) + \
               "Period: {}\n".format(self.period) + \
               "Metric: {}\n".format(self.metric) + \
               "Start: {}\n".format(self.start) + \
               "Stop: {}\n".format(self.stop) + \
               "Ordering: {}\n".format(self.ordering) + \
               "Club Level: {}\n".format(self.club_level) + \
               "Statistic: {}\n".format(self.statistic) + \
               "SQL Query: {}\n\n".format(self.sql_string) + \
               "SQL metric: {}\n".format(self.sql_metric) + \
               "SQL factors: {}\n".format(self.sql_factors) + \
               "SQL period: {}\n".format(self.sql_period) + \
               "SQL ordering: {}\n".format(self.sql_ordering) + \
               "SQL start: {}\n".format(self.sql_start) + \
               "SQL stop: {}\n".format(self.sql_stop)

    def translate_to_sql(self):
        if not self.sql_period:
            self.sql_period = translation_dictionary.get(self.period, self.period)

        if not self.sql_metric:
            if not self.metric:
                self.sql_metric = DEFAULT_METRIC
            else:
                self.sql_metric = self.metric

        self.sql_factors += [translation_dictionary.get(x, x) for x in self.factors]
        self.sql_factors = list(set(self.sql_factors))
        self.sql_factors.sort()

        if not self.sql_ordering:
            self.sql_ordering = translation_dictionary.get(self.ordering, self.ordering)

        if not self.sql_start:
            if not self.start:
                self.sql_start = DEFAULT_START.strftime("%Y-%m-%d-00-00-00-000")
            else:
                self.sql_start = self.start.strftime("%Y-%m-%d-00-00-00-000")

        if not self.sql_stop:
            if not self.stop:
                self.sql_stop = DEFAULT_STOP.strftime("%Y-%m-%d-00-00-00-000")
            else:
                self.sql_stop = self.stop.strftime("%Y-%m-%d-23-59-59-999")

        if self.club_level:
            self.sql_club_level = """AND clublevel = '{}'""".format(translation_dictionary.get(self.club_level, self.club_level))
        else:
            self.sql_club_level = ''

        if self.statistic:
            self.sql_statistic = """SELECT {}(metric) FROM (""".format(translation_dictionary.get(self.statistic, self.statistic))
        else:
            self.sql_statistic = ''

    @helper.timeit
    def generate_query_params_from_response(self, query, response, error_checking = False):
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

        # Add query to query parameters object
        self.query = query

        # Look at intent
        self.intent = response_dict['intents'][0]['intent']
        self.intent_confidence = response_dict['intents'][0]['confidence']

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
                self.period = entity['value']
            if entity['entity'] == 'both_metrics':
                self.metric = entity['value']
            if entity['entity'] == 'top':
                self.ordering = entity['value']
            if entity['entity'] == 'club_level':
                self.club_level = entity['value']
            if entity['entity'] == 'statistics':
                self.statistic = entity['value']
            if entity['entity'] == 'sys-date':
                year = int(entity['value'][:4])
                month = int(entity['value'][5:7])
                day = int(entity['value'][8:])
                date = datetime(year, month, day, tzinfo = TIME_ZONE)
                date_list.append(date)

        # Add factor list to query parameters
        self.factors = list(set(factor_list))

        # Find start and stop from date entities
        if date_list:
            self.start = min(date_list)
            self.stop = max(date_list)
        self.stop = self.stop + timedelta(hours = 23,
                                          minutes = 59,
                                          seconds = 59,
                                          milliseconds = 999)

    # @helper.timeit
    # def generate_sql_query(self):
    #     # Translate entities to SQL
    #     self.translate_to_sql()
    #
    #     # Create string of factors
    #     factors_string = ''
    #     additional_group_by = ''
    #     ctr = 0
    #     for factor in self.sql_factors:
    #         factors_string += ', '
    #         factors_string += factor
    #         additional_group_by += ', ' + str(3 + ctr)
    #         ctr += 1
    #
    #     # Create string formatting
    #     if self.sql_statistic:
    #         suffix = ') AS t'
    #     else:
    #         suffix = ''
    #
    #     # Create SQL query
    #     SQL_string = \
    #         """{}SELECT {} AS metric, date_trunc('{}', {}.tmstmp) AS tmstmp{}
    #            FROM {}
    #            WHERE tmstmp >= to_timestamp('{}', 'YYYY-MM-DD-HH24-MI-SS-MS')
    #            AND tmstmp <= to_timestamp('{}', 'YYYY-MM-DD-HH24-MI-SS-MS') {}
    #            GROUP BY 2{}
    #            {}{}""".format(self.sql_statistic,
    #                           self.sql_metric,
    #                           self.sql_period,
    #                           DATABASE_TABLE,
    #                           factors_string,
    #                           DATABASE_TABLE,
    #                           self.sql_start,
    #                           self.sql_stop,
    #                           self.sql_club_level,
    #                           additional_group_by,
    #                           self.sql_ordering,
    #                           suffix)
    #
    #     self.sql_string = SQL_string

    @helper.timeit
    def generate_sql_query(self, error_checking = False):
        # Translate entities to SQL
        self.translate_to_sql()

        # Create string of factors
        factors_string = ''
        additional_group_by = ''
        ctr = 0
        for factor in self.sql_factors:
            factors_string += ', '
            factors_string += factor
            additional_group_by += ', ' + str(3 + ctr)
            ctr += 1

        # Create string formatting
        if self.sql_statistic:
            suffix = ') AS t'
        else:
            suffix = ''

        # Find correct table to get data from
        title_string = self.sql_period + '_factored_by'
        for factor in self.sql_factors:
            title_string += '_'
            title_string += factor
        if error_checking:
            print "Database table name: {}".format(title_string)

        # Create SQL query
        SQL_string = \
            """{}SELECT {} AS metric, {} AS tmstmp{}
               FROM {}
               WHERE {} >= to_timestamp('{}', 'YYYY-MM-DD-HH24-MI-SS-MS')
               AND {} <= to_timestamp('{}', 'YYYY-MM-DD-HH24-MI-SS-MS')
               {}{}""".format(self.sql_statistic,
                              self.sql_metric,
                              self.sql_period,
                              factors_string,
                              title_string,
                              self.sql_period,
                              self.sql_start,
                              self.sql_period,
                              self.sql_stop,
                              self.sql_ordering,
                              suffix)
        if error_checking:
            print "SQL string: {}".format(SQL_string)

        self.sql_string = SQL_string



if __name__ == "__main__":
    # query = 'games played by area january 1st 2015?'
    query = 'what is my hourly revenue by club level, area, zone, stand, wager, manufacturer, game title'
    query = 'what is the payout rate for january 2 2015'
    query = 'by minute january 2nd 2015 revenue by club level, bank, zone'
    response = get_intent_entity_from_watson(query)

    # Create query parameters object
    query_params = query_parameters()
    query_params.generate_query_params_from_response(query, response)
    query_params.generate_sql_query(error_checking = True)

    # Run SQL query
    engine = helper.connect_to_database(DATABASE_USER, DATABASE_DOMAIN, DATABASE_NAME)
    df = helper.get_sql_data("""SELECT * FROM logs LIMIT 10;""", engine)
    print df.head()
    raw_input()
    df = helper.get_sql_data(query_params.sql_string, engine)
    print df.head(10)
    print len(df)
