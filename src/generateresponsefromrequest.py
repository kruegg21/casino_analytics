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

'''
The purpose of this module is to send out a query (that has been cleaned)
to the nlpapi, and get a response back in the form of
'''

import helper
import requests

def spell_check_word(word):
    '''
    Args:
        word (str): this is the word being spell checked
    Returns:
        spell_checked_word (str):
    '''
    spell_checked_word = word
    return spell_checked_word


def spell_check_query(query):
    '''
    Args:
        query (str): this is the natural language string input that the user
        is going to put into the front end of the application
    Returns:
        spell_checked_queries (str):
    '''
    spell_checked_query = query
    return spell_checked_query

def tokenize_query(query):
    '''
    Inputs:
        query (str): this is the natural language string input that the user
        is going to put into the front end of the application
    Outputs:
        tokenized_query (str): this is the query that has going through the
        following text processing functions:
        a) spell checking
        b) replacement of spaces with "+" signs
    '''

    if len(query) < 1:
        return 'a valid string needs to be put in'
    spell_checked_query = query
    tokenized_query = spell_checked_query.replace(' ', '+')
    return tokenized_query

@helper.timeit
def get_intent_entity_from_watson(query, error_checking = False):
    '''
    Inputs:
        query (str): this is the natural language string input that the user
        is going to put into the front end of the application
    Outputs:
        sqlquery (str): this is the output string that is going to be produced
        by the application
    '''
    tokenized_query = tokenize_query(query)
    watson_string = 'https://gaminganalyticsai-host.mybluemix.net/{}'.format(tokenized_query)
    response = requests.get(watson_string)

    if error_checking:
        print 'Query being sent to Watson: {}'.format(watson_string)

    if response.status_code != 200:
        print 'WARNING', response.status_code
    else:
        return response.json()


if __name__ == "__main__":
    query = 'what is my daily revenue by club level, game title, manufacturer, zone, bank, stand, wager, club level'
    response = get_intent_entity_from_watson(query)
    print response
