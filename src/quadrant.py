class quadrant(object):
    '''
    Quadrant objects contain the data and specify how a plot or table should
    be produced for a quadrant of the visualization page
    '''
    def __init__(self, viz_type = 'plot'):
        self.viz_type = viz_type
        self.title = None
        self.plot = None
        self.df = None
        self.table_data = []
        self.column_titles = []
