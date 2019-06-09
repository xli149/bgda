from runstats import Statistics
# from runstats import Regression


class FeatureBin:

    def __init__(self, feature):
        self.feature = feature

        # A list of Statistics() obj each represents the stats of a day
        self.days_stats = [Statistics() for i in range(366)]

        # 366 lists of size 24 where each item in the inner list represents the statistics of the hour
        # nested for easy access by day number
        self.hours_stats = [[Statistics() for j in range(24)] for i in range(366)]

        # A list of 12 Statistics() obj each represents the stats of the month
        self.months_stats = [Statistics() for i in range(12)]

        # Linear Regression of the value vs. time, for rate of change
        # self.tm_regr = Regression()

    def update(self, val, tm):
        nth_day = tm.tm_yday - 1
        nth_month = tm.tm_mon - 1
        nth_hour = tm.tm_hour

        self.days_stats[nth_day].push(val)
        self.hours_stats[nth_day][nth_hour].push(val)
        self.months_stats[nth_month].push(val)

    # This could be potentially useful
    def merge(self, bin):
        # Merge day stats
        for i in range(len(self.days_stats)):
            self.days_stats[i] += bin.days_stats[i]

        # TODO: Add merge month stats and hour stats
