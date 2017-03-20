from quadrant import quadrant
from translation_dictionaries import *
from query_parameters import query_parameters
import visualizations
import helper
import pandas as pd

main_factors = ['bank', 'zone', 'clublevel', 'area']

def netwin_analysis(df, query_params, engine):
    # Metrics holder
    metrics = {}

    '''
    Top left plot
    '''
    # Create quadrant object
    tl_quadrant = quadrant(viz_type = 'plot')

    # Group by time period to get totals of metric
    #   tmstmp        metric
    # 0 2015-04-01  1.776029e+06
    # 1 2015-05-01  1.807870e+06
    # 2 2015-06-01  1.766107e+06
    # 3 2015-07-01  1.845381e+06
    # 4 2015-08-01  1.870472e+06
    df_total = helper.sum_by_time(df, None)
    total = helper.convert_money_to_string(df_total.iloc[-1].metric)

    # Convert metric to PUPD adjucted metric
    #   tmstmp      metric
    # 0 2015-04-01  298.391909
    # 1 2015-05-01  303.741582
    # 2 2015-06-01  296.724972
    # 3 2015-07-01  310.043796
    # 4 2015-08-01  314.259388
    df_pupd = helper.calculate_pupd(df_total, query_params)
    pupd = helper.convert_money_to_string(df_pupd.iloc[-1].metric)

    # Add total and PUPD text to metrics dictionary
    readable_period = human_readable_translation[query_params.period]
    metrics['Number of Machines'] = query_params.num_machines
    metrics['Total Net Win for this {}'.format(readable_period)] = total
    metrics['Net Win PUPD for this {}'.format(readable_period)] = pupd

    # Make plot
    tl_quadrant.plot = visualizations.makeplot('line', df_pupd, query_params, metrics)

    '''
    Top right table
    '''
    # Create quadrant object
    tr_quadrant = quadrant(viz_type = 'table')

    # Make column titles
    column_titles = [(readable_period),
                     ('Net Win PUPD')]
    title = ('Net Win PUPD by {}'.format(readable_period),)

    # Create table data
    table_data = []
    for i in xrange(1, len(df_pupd) + 1):
        tmstmp = helper.convert_datetime_to_string(df_pupd.iloc[-i]['tmstmp'], query_params.sql_period)
        metric = helper.convert_money_to_string(df_pupd.iloc[-i]['metric'])
        table_data.append((tmstmp, metric))
    tr_quadrant.title = title
    tr_quadrant.column_titles = column_titles
    tr_quadrant.table_data = table_data

    '''
    Bottom left table:
    Net win by bank analysis. There is opportunity to speed this up by only
    SQL querying data from the most recent period.
    '''
    factor = 'bank'
    machines_per_bank = 4

    # Create quadrant object
    bl_quadrant = quadrant(viz_type = 'table')

    # Generate new query params object
    query_params_bl = query_parameters()
    query_params_bl.start = query_params.start
    query_params_bl.stop = query_params.stop
    query_params_bl.period = query_params.period
    query_params_bl.factors = main_factors

    # Generate new SQL query
    query_params_bl.generate_sql_query()

    # Pull data down
    df = helper.get_sql_data(query_params_bl.sql_string, engine)

    # Get data from most recent period
    most_recent_period = df.iloc[-1].tmstmp
    df_recent = df[df.tmstmp == most_recent_period]

    # Group and sum by time period and factor
    #   tmstmp   factor       metric
    # 0 2017-03-01   BANK-1  121500.8000
    # 1 2017-03-01  BANK-10    1987.4550
    # 2 2017-03-01  BANK-11  103462.5950
    # 3 2017-03-01  BANK-12   58532.7836
    # 4 2017-03-01  BANK-13    4754.6033
    df_total = helper.sum_by_time(df_recent, factor)

    # Calculate PUPD adjusting for fact we have 4 machines per bank
    df_pupd = helper.calculate_pupd(df_total, query_params_bl)
    df_pupd.metric = df_pupd.metric * (float(192) / machines_per_bank)

    # Find House Average
    house_average = helper.convert_money_to_string(df_pupd.metric.mean())

    # Sort by Net Win PUPD
    df_pupd.sort_values('metric', inplace = True)

    # Create table
    title = ('Net Win by Bank this {}'.format(readable_period),)
    column_titles = ('Bank', 'Total Net Win', 'Net Win PUPD', 'House Average')
    table_data = []
    for i in xrange(1, len(df_pupd) + 1):
        table_data.append((df_pupd.iloc[-i]['factor'],
                           helper.convert_money_to_string(df_pupd.iloc[-i]['total']),
                           helper.convert_money_to_string(df_pupd.iloc[-i]['metric']),
                           house_average))
    bl_quadrant.title = title
    bl_quadrant.column_titles = column_titles
    bl_quadrant.table_data = table_data

    '''
    Bottom right table
    '''
    # Create quadrant object
    br_quadrant = quadrant(viz_type = 'table')

    # Create new query params object
    query_params_br = query_parameters()
    query_params_br.start = query_params.start
    query_params_br.stop = query_params.stop
    query_params_br.period = query_params.period
    query_params_br.factors = ['assetnumber', 'assettitle']

    # Generate SQL query
    query_params_br.generate_sql_query(error_checking = False)

    # Pull data down
    df = helper.get_sql_data(query_params_br.sql_string, engine)

    # Select only current month data
    current_month = df.iloc[-1].tmstmp
    df_current = df[df.tmstmp == current_month].sort_values('metric')

    # Calculate PUPD
    # metric   tmstmp      assetnumber assettitle                     total
    # 2.884023 2017-03-01  AST-000107  Sweet 3                        89.4047
    # 2.937384 2017-03-01  AST-000174  Michael Jackson Icon           91.0589
    # 3.034935 2017-03-01  AST-000096  Wheel Of Fortune               94.0830
    # 3.081771 2017-03-01  AST-000124  Lucky Larry's Lobstermania     95.5349
    # 3.125777 2017-03-01  AST-000170  Playboy Dont Stop The Party    96.8991
    df_pupd = helper.calculate_pupd(df_current, query_params)
    df_pupd['metric'] = df_pupd.metric * 192

    # Get column titles
    column_titles = ('Machine Number', 'Title', 'Best/Worst', 'Net Win PUPD')
    title = ('Best and Worst Machines for {}'.format(readable_period),)

    # Get table data
    table_data = []
    for i in xrange(1,6):
        row = df_pupd.iloc[-i]
        table_data.append((row.assetnumber, row.assettitle, 'Best', helper.convert_money_to_string(row.metric)))
    for i in xrange(5):
        row = df_pupd.iloc[i]
        table_data.append((row.assetnumber, row.assettitle, 'Worst', helper.convert_money_to_string(row.metric)))
    br_quadrant.column_titles = column_titles
    br_quadrant.title = title
    br_quadrant.table_data = table_data

    return tl_quadrant, tr_quadrant, bl_quadrant, br_quadrant
