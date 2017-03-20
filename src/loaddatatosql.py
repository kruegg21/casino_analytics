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
The purpose of this script is to load the playlogs.csv file into a postgres
database in your local, inside a database named playlogs.

Script to do this using the command "python src/loaddatatosql.py" from the
root directory

These are the ff requirements for it to work

1. postgres is installed on the machine, with a user "test" with a
password "test"
2. pandas, sqlalchemy are installed
3. "playlogs.csv" is in the data directory, and this script is inside a
sister directory named src
4. this script is run from the root directory
>root
>>data
>>src
5. a database named "playlogs" has been created in postgres with the command
CREATE DATABASE playlogs;
'''

import os
import json
import pandas as pd
from sqlalchemy import create_engine
import multiprocessing as mp

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

def split_list(doc_list, n_groups):
    """
    Args:
        doc_list (list): is a list of documents to be split up
        n_groups (int): is the number of groups to split the doc_list into
    Returns:
        split_lists (list): a list of n_groups which are approximately equal
        in length, as a necessary step prior to multiprocessing
    """
    avg = len(doc_list) / float(n_groups)
    split_lists = []
    last = 0.0
    while last < len(doc_list):
        split_lists.append(doc_list[int(last):int(last + avg)])
        last += avg
    return split_lists


def convert_datestring_list(split_docs):
    """
    Args:
        split_docs (list): list of datestrings to be converted
    Returns:
        converted datestrings (list): a list of converted datestrings
    """
    return [convert_datestring_to_pydatetime(text) for text in split_docs]


def convert_datestring_to_pydatetime(datestring):
    '''
    Args:
        datestring (str): this is a string for a date with the format
        MM/DD/YYY
    Returns:
        pydt (python datetime): this is the string converted into the python
        date time
    '''
    pydt = pd.to_datetime(datestring).to_pydatetime()
    return pydt


def multiprocess_convert_datestrings(datestrings):
    """
    Args:
        datestrings (list): this is a list of the datestring string items
    Returns:
        converted_datestrings (list): a list of converted datestring string
        items done with multiprocessing
    """
    n_processes = mp.cpu_count()
    p = mp.Pool(n_processes)
    split_docs = split_list(datestrings, n_processes)
    converted_datestrings = p.map(convert_datestring_list, split_docs)
    return [item for row in converted_datestrings for item in row]


def process_playlogs_data(df):
    '''
    Args:
        df (dataframe): this is the raw playlogs data
    Returns:
        df (dataframe): this is the playlogs data where the ff procedures
        have been run:
        1. timeStamp dropped, renamed to tmstmp
        2. column names lowered
        3. timestamp strings converted into py datetime objects for the
        columns [tmstmp and installed]
    '''
    df = df.rename_axis({"timeStamp": "tmstmp"}, axis="columns")
    df.columns = [c.lower() for c in df.columns]

    # Drop useless columns
    if '_id' in df.columns:
        df.drop('_id', axis = 1, inplace = True)

    if '__v' in df.columns:
        df.drop('__v', axis = 1, inplace = True)

    # Turn time columns into readable format
    df.tmstmp = pd.to_datetime(df.tmstmp, infer_datetime_format = True)
    df.installed = pd.to_datetime(df.installed, infer_datetime_format = True)

    return df

def create_table(engine):
    # Connect to engine
    connection = engine.connect()

    # Make table
    SQL_string = """CREATE TABLE current_logs (
                    assetNumber      varchar(12),
                    assetTitle       varchar(60),
                    manufacturer     varchar(12),
                    zone varchar(12),
                    area varchar(12),
                    bank varchar(12),
                    stand varchar(12),
                    assetCost integer,
                    installed timestamp,
                    denom double precision,
                    accountNumber varchar(12),
                    clubLevel varchar(10),
                    playType varchar(10),
                    gamesPlayed integer,
                    gamesWon integer,
                    amountBet double precision,
                    amountWon double precision,
                    tmstmp timestamp
                    );"""
    result = connection.execute(SQL_string)

def load_data_to_sql(engine):
    # Connect to engine
    connection = engine.connect()

    SQL_string = """COPY films FROM '/Users/kruegg/Desktop/refactored_casino_analytics/data/playlogs.csv' DELIMITERS ',' CSV;"""
    result = connection.execute(SQL_string)

if __name__ == "__main__":
    # Connect to database
    database_string = 'postgres://{}:{}@{}:{}/{}'.format(DATABASE_USER,
                                                         DATABASE_PASSWORD,
                                                         DATABASE_HOST,
                                                         DATABASE_PORT,
                                                         DATABASE_NAME)
    engine = create_engine(database_string)
    create_table(engine)

    # dataset_name = 'playlogs0318_01.csv'
    # df = pd.read_csv('../data/{}'.format(dataset_name))
    # df = process_playlogs_data(df)
    # df.to_csv('{}_processed.csv'.format(dataset_name), index = False)

    # df = pd.read_csv('../data/playlogs.csv')
    # print('loaded dataframe')
    # df = process_playlogs_data(df)
    # print('finished processing dataframe')
    # engine = \
    #     create_engine('postgres://ohdimqey:KWukvWaldGeiUc3XzP5Tu3NnwdBOw6Ya@soft-feijoa.db.elephantsql.com:5432/ohdimqey')
    # df.to_sql('logs', engine, index=False, chunksize=100000)
    # print('finished saving to postgres')
