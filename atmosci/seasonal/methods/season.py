
import datetime

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SeasonDateMethods:

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def appDateFormat(self, date):
        return date.strftime('%Y-%m-%d')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def currentSeason(self):
        return self.targetYear(today)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dayToSeasonDate(self, season, day):
        end_day = self.seasonEndDay()
        season_start = self.seasonStartDate(season)
        start_month = season_start.month

        # season starts & ends in same year
        if start_month < end_day[0]:
            target_year = season_start.year + 1
        else: # season starts in year before it ends
            if day[0] < end_day[0]:
                # day is in same year as end date
                target_year = season_start.year + 1
            else:
                # day is in same year as start date
                target_year = season_start.year
        return datetime.date(target_year, *day)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def seasonDates(self, year_or_date):
        if isinstance(year_or_date, int):
            end_date = self.seasonEndDate(year_or_date)
            start_date = self.seasonStartDate(year_or_date)
        if isinstance(year_or_date, (datetime.date, datetime.datetime)):
            end_date = self.seasonEndDate(year_or_date.year)
            start_date = self.seasonStartDate(year_or_date.year)
        return (end_date.year, start_date, end_date)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def seasonEndDate(self, target_year):
        # by definition, the target year is always the year the season ends
        return datetime.date(target_year, *self.seasonEndDay())

    def seasonStartDate(self, target_year):
        end_day = self.seasonEndDay()
        start_day = self.seasonStartDay()
        # start/end dates should be in same year when start month < end month
        if start_day[0] < end_day[0]:
            return datetime.date(target_year, *start_day)
        # start and end date are in different years
        # end date is always in target year, so start date is in previous year
        return datetime.date(target_year-1, *start_day)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def seasonEndDay(self):
        return self.project.get('season_end_day',
               self.project.get('end_day', None))

    def seasonStartDay(self):
        return self.project.get('season_start_day',
               self.project.get('start_day', None))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def targetYear(self, date):
        end_day = self.seasonEndDay()
        season_start = self.seasonStartDate(date.year)
        # aseason starts and end in same year
        if season_start.month < end_day[0]: return season_start.year
        # start and end dates are in different years
        return season_start.year+1

