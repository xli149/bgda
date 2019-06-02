from runstats import Statistics
# from runstats import Regression


class FeatureBin:

    def __init__(self, feature):
        self.feature = feature

        # Month stats are essentially sum of all day stats in the month
        # TODO: Even lower base resolution than day? hours?
        # Lower res -> More Statistics() -> More memory & Expensive high res summation (get year max)
        # May be multi-res to avoid Expensive high res summation at the cost of memory and insertion time?
        self.days_stats = [Statistics() for i in range(366)]


        # TODO: Add running linear regression in future
        # self.regr = Regression()

    def update(self, val, day):
        self.days_stats[day].push(val)

    # This could be potentially useful
    def merge(self, bin):
        for i in range(len(self.days_stats)):
            self.days_stats[i] += bin.days_stats[i]
