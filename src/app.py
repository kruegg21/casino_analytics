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

import os
import json

from flask import Flask, jsonify, render_template, redirect, session, url_for
from flask import make_response
from flask import request
from flask_wtf import Form
from wtforms import TextAreaField, SubmitField
from wtforms.validators import Required

from watson_developer_cloud import WatsonException
from main import main


app = Flask(__name__)
port = os.getenv('PORT', '8080')


@app.route('/')
def home():
    '''
    Args:
        natural language query on the data
    Returns:
        routes to the visualization page
    '''
    return render_template('landing.html')


@app.route('/visualizations', methods=['GET', 'POST'])
def search_and_viz():
    '''
    Args:
        plot1 (unicode): this is the html, css, javascript to render the
        mpld3 plot
        mainfactors (list): this is a list of tuples where each element
        is three items - the metric, the direction, and the percent change
        plot2 (unicode): this is the html, css, javascript to render the
        mpld3 plot
        derivedmetrics (list): this is a list of tuples where each element
        is three items - the metric, the direction, and the percent change
    Returns:
        visualized plot1, plot2, mainfactors, and derivedmetrics
        on the same page
    '''
    query = str(request.form['user_input'])
    plot1, plot2, mainfactors, derivedmetrics, metrics = main(query)
    return render_template('index.html', plot1=plot1, plot2=plot2,
                           mainfactors=mainfactors,
                           derivedmetrics=derivedmetrics,
                           statistics=metrics)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(port), debug=False)
