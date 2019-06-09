import math
import runstats
import traceback
from runstats import Regression

"""

This class is a subsitute of StreamCorrelationMatrix. Using runstats Regression()
and maintaining a matrix of single linear regression. Regression also provides
slope and intercept, which are also served through the

slope(attribute_a, attributes_b) and
intercept(attribute_a, attribute_b) method
"""

hardcoded_columns = ['UTC_DATE',
                     'UTC_TIME',
                     'LONGITUDE',
                     'LATITUDE',
                     'AIR_TEMPERATURE',
                     'PRECIPITATION' ,
                     'SOLAR_RADIATION' ,
                     'SURFACE_TEMPERATURE',
                     'RELATIVE_HUMIDITY']


class LinearRegressionMatrix:

    def __init__(self, columns=hardcoded_columns):
        self.columns = columns
        # Added for easy lookup, othewise columns.index() is O(n)
        self.index = {columns[i] : i for i in range(len(columns))}
        print(self.index)

        self.matrix = [[Regression() for c in columns] for c in columns]



    def add(column):
        pass
        # TODO: Add another column to the matrix on the fly
        # 1. update self.columns
        # 2. insert self.index with new columns
        # 3. update matrix (both existing inner list and new inner list)

    def update(self, record):
        #remove NULL missing records
        record = {k: v for k, v in record.items() if v is not None}

        for k1, v1 in record.items():
            for k2, v2 in record.items():
                self._update(k1, v1, k2, v2)

    def _update(self, col_a, val_a, col_b, val_b):
        a_index = self.index[col_a]
        b_index = self.index[col_b]
        self.matrix[a_index][b_index].push(val_a, val_b)


    def correlation(self, attribute_a, attribute_b):
        return self.matrix[self.index[attribute_a]][self.index[attribute_b]].correlation()

    def get_matrix(self):
        m = []
        for i, row in enumerate(self.matrix):
            nr = []
            for j, r in enumerate(row):
                if i == j:
                    nr.append(1)
                    continue
                try:
                    nr.append(r.correlation())
                except:
                    # A divided by zero exception
                    # This only happens if there is only one x value in the regression, hardcoded 0 correlation
                    nr.append(0)
            m.append(nr)
        return m
