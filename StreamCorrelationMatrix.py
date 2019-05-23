import math

class StreamCorrelationMatrix:
    def __init__(self):
        self.columns = ['UTC_DATE',
                        'UTC_TIME',
                        'LONGITUDE',
                        'LATITUDE',
                        'AIR_TEMPERATURE',
                        'PRECIPITATION' ,
                        'SOLAR_RADIATION' ,
                        'SURFACE_TEMPERATURE',
                        'RELATIVE_HUMIDITY']
        self.n = 0
        self.means = [0 for _ in self.columns]
        self.cov_sums = [[0 for _ in self.columns] for _ in self.columns]
        self.var_sums = [0 for _ in self.columns]
        self.rs = [[0 for _ in self.columns] for _ in self.columns]

    def update(self, record):
        assert list(record.keys()) == self.columns

        # update means
        for i, col in enumerate(self.columns):
            # if col ==  'AIR_TEMPERATURE' or col == 'SURFACE_TEMPERATURE':
            #     print()
            self.means[i] = (self.means[i] * self.n + record[col]) / (self.n + 1)
            self.var_sums[i] += (record[col] - self.means[i]) ** 2


        # update variance sums
        for i, col_o in enumerate(self.columns):
            for j, col_i in enumerate(self.columns):
                self.cov_sums[i][j] += (record[col_o] - self.means[i]) * (record[col_i] - self.means[j])

                denom = math.sqrt(self.var_sums[i]) * math.sqrt(self.var_sums[j])

                self.rs[i][j] = (self.cov_sums[i][j] / denom) if denom else 1.0
        self.n += 1

    def get_correlation(self, attribute_a, attribute_b):
        assert attribute_a in self.columns
        assert attribute_b in self.columns
        a_index = self.columns.index(attribute_a)
        b_index = self.columns.index(attribute_b)

        return self.rs[a_index][b_index]

    def get_matrix(self):
        return self.rs