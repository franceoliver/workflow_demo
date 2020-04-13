import pandas as pd
import datetime as dt
from ._db_data import DBData


class RDA(DBData):
    """A class that contains all the Rapid Diagnostic Analytics tests"""

    def __init__(self):
        super().__init__()
        db_obj = DBData()
        # assign class variables
        self.df_ta = db_obj.retrieve_data('cln_shift')
        self.df_pa = db_obj.retrieve_data('cln_payroll')
        self.df_master = db_obj.retrieve_data('cln_emp_master')

    def test_1(self):
        """Payslips: Annualised salaries - Detect indicators of employee on annualised salary & compare to \
        “like for like” employees paid hourly rates."""
        # Aggregate salaried employees net income and hours worked
        # Find like for like hourly worker and times that amount by the total salaried employees hour and see difference.

        # I've got employee master data in the folder.

        # Brining in payroll dataset & master dataset
        df_pa = self.df_pa.__deepcopy__()
        df_master = self.df_master.__deepcopy__()

        # Creating a list of just Salary employees
        salary_emp = list(df_pa['emp_id'].loc[df_pa['pay_type']=='SAL'])
        # Removing duplicates from these emp_ids'
        def Remove(duplicate):
            final_list = []
            for num in duplicate:
                if num not in final_list:
                    final_list.append(num)
            return final_list

        salary_emp = Remove(salary_emp)

        # filtering the payroll data to include just salaried employees
        df_sal_emp = df_pa.loc[df_pa['emp_id'].isin(salary_emp)]
        # Filtering to remove all pay_types said to be excluded from the dataset
        def sal_groupby(df_sal_emp):
            df_sal_emp_exc = df_sal_emp.loc[df_sal_emp['mapping_inclusion'] != 'Exclude']
            # Aggregating by emp_id to give total pay and total hours
            agg_df_sal_emp = df_sal_emp_exc.groupby(['emp_id', 'position_name', 'level', 'employment_status', 'venue']).agg(
                total_pay=pd.NamedAgg(column='period_amount', aggfunc=sum),
                total_hours=pd.NamedAgg(column='hours_for_period', aggfunc=sum)).reset_index()
            # Adding in the amount per hour worked
            agg_df_sal_emp['amount_per_hour'] = agg_df_sal_emp['total_pay'] / agg_df_sal_emp['total_hours']
            return agg_df_sal_emp

        # Group by for salaried employees
        agg_df_sal_emp = sal_groupby(df_pa.loc[df_pa['emp_id'].isin(salary_emp)])
        # Adding a dummy key to show emp is salary
        agg_df_sal_emp['is_emp_sal'] = 1

        # Group by for non salaried employees
        agg_df_non_sal_emp = sal_groupby(df_pa.loc[~df_pa['emp_id'].isin(salary_emp)])
        # Adding a dummy key to show emp is NOT salary
        agg_df_non_sal_emp['is_emp_sal'] = 0

        # Aggregating together
        agg_df_results = agg_df_sal_emp.append(agg_df_non_sal_emp)
    
        # Returning converted to dict 
        return agg_df_results.to_dict(orient='list')

    def test_2(self):
        """Payslips: “Fully loaded” flat rates - Detect indicators of employee on loaded flat rates & compare to \
         “like for like” employees paid hourly rates."""
        # For rockpool we dont have this!
        pass

    def test_3(self):
        """Payslips: Allowance consumption - Look for “like for like” employment and assess consistency of pay element \
        consumption across the population."""
        # within the payroll data key we have a flag for allowances
        # Sum the allowances for each employee across the entire period
        # Give a total of the hours work for period number of units of allowance awarded to them

        # Brining in payroll dataset.
        df_pa = self.df_pa.__deepcopy__()

        # Filtering for just the allowances
        df_pa = df_pa.loc[df_pa['is_allowance'] == 'y']

        # aggregating over emp_id
        allowance_agg_df = df_pa.groupby(['emp_id', 'position_name', 'mapping_codes', 'mapping_description']).agg(
            total_allowance_paid=pd.NamedAgg(column='period_amount', aggfunc=sum),
            total_allowance_hours=pd.NamedAgg(column='hours_for_period', aggfunc=sum)).reset_index()

        return allowance_agg_df.to_dict(orient='list')

    def test_4(self):
        """Payslips: Inaccurate classification or inconsistent rates - Look for “like for like” employment and \
        determine deviation from mode rates paid at classification."""
        # Group role, hr rate and count of all employees across data set.
        # e.g if we have a cook who is being paid differently than all the others!

        # Brining in payroll dataset.
        df_pa = self.df_pa.__deepcopy__()

        # Filtering for just the includes work codes as given by the rockpool logic
        df_pa_inc = df_pa.loc[df_pa['mapping_inclusion'] != 'Exclude']

        # Aggregating results.
        df_pa_inc_agg = df_pa_inc.groupby(['emp_id', 'position_name']).agg(
            total_pay=pd.NamedAgg(column='period_amount', aggfunc=sum),
            total_hours=pd.NamedAgg(column='hours_for_period', aggfunc=sum)).reset_index()
        # Adding in the amount per hour worked
        df_pa_inc_agg['amount_per_hour'] = df_pa_inc_agg['total_pay'] / df_pa_inc_agg['total_hours']

        return df_pa_inc_agg.to_dict(orient='list')

    def test_5(self):
        """Payslips: Superannuation configuration and interpretation - Independent projection of super contributions \
        and compare to actual payments. Challenge interpretations."""
        # Map which payments should have super
        # and then reconcile it to actual super payments
        # However Rockpool dont have super in their data so can't do.
        pass

    def test_6(self):
        """Time & attendance: Employee “casualness” - Determine the regularity of employee working patterns rate an \
        employee’s likelihood to be casual/non-casual."""
        # Layout: if employees are working the same rough hours on each day consistently.
        weekday = ['Mon', 'Tue', 'Wed', 'Thur', 'Fri', 'Sat', 'Sun']

        df_ta = self.df_ta.__deepcopy__()

        df_ta['shift_date'] = df_ta['shift_start'].dt.date.apply(lambda x: x.strftime('%Y-%m-%d'))

        # Calculating the length of the shift in minutes
        df_ta['shift_len_mins'] = (df_ta['shift_end'] - df_ta['shift_start']).dt. \
            total_seconds().div(60).astype(int)

        # The day of the week with Monday=0, Sunday=6. I have changed to str for analysis
        df_ta['day_of_week'] = df_ta['shift_start'].dt.dayofweek.astype(int).apply(lambda x: weekday[x])

        # Dummy to show if shift starts in AM or PM
        df_ta['am'] = df_ta['shift_start'].apply(lambda x: 'am' if x.time() < dt.time(12) else 'pm')

        # creating a concat to show day and AM or PM of shift
        df_ta['shift_overview'] = df_ta['day_of_week'] + '_' + df_ta['am']

        # Creating concat to feed into remove duplicates to get rid of split shifts per data and AM or PM e.g
        # Someone works two PM shifts
        df_ta['emp_shift_date_am_pm'] = df_ta['emp_id'] + '_' +\
                                        df_ta['shift_date'] + \
                                        '_' + df_ta['am']

        # Taking a snap shot of df_ta to be returned before deduplication to give a calendar heat map.
        # df_ta['shift_start'] = df_ta['shift_start'].apply(lambda x: x.strftime('%d/%-m/%y' '%H:%M:%S'))
        # df_ta['shift_end'] = df_ta['shift_end'].apply(lambda x: x.strftime('%d/%-m/%y' '%H:%M:%S'))
        cal_heat_map = df_ta[:]

        return cal_heat_map.to_dict()


    def test_7(self, shift_duration_hrs, min_break_duration_mins):
        """Time & attendance: Rest and meal breaks - Analyse shift patterns and timing and length of breaks across \
        employee cohorts to find potentially missing entitlements."""

        # If employees are taking the required break each shift
        # with tsid_start_date being the same day the break is calculated by the time between the end of the last shift
        # and the start of the next shift.
        # two parameters.. length of shift worked, length of break

        # Output - Which employees arnt taking the breaks
        df_ta = self.df_ta.__deepcopy__()
        # Creating the shift_date column for anlysis
        df_ta['shift_date'] = df_ta['shift_start'].dt.date.apply(lambda x: x.strftime('%Y-%m-%d'))

        # Sort by emp_id, shift date, shift start time
        df_ta = df_ta.sort_values(by=['emp_id', 'shift_start'], ascending=True)
        # Get shift start and end time for each employee on each day
        shifts = df_ta.groupby(['emp_id', 'shift_date']).agg({'shift_start': 'min', 'shift_end': 'max'})
        shifts.columns = ['min_shift_start', 'max_shift_end']
        shifts = shifts.reset_index()

        shifts['max_shift_end'] = pd.to_datetime(shifts['max_shift_end'])
        shifts['min_shift_start'] = pd.to_datetime(shifts['min_shift_start'])

        # Get net shift duration
        shifts['shift_dur'] = shifts['max_shift_end'] - shifts['min_shift_start']  # 'timedelta' shift duration
        shifts['shift_dur'] = shifts['shift_dur'].dt.total_seconds().div(3600)  # convert timedelta to hours float

        # Flag if employee was entitled to a break (shift length >= 6 hours)
        shifts['break_flag'] = 0
        shifts.loc[shifts['shift_dur'] >= shift_duration_hrs, 'break_flag'] = 1

        # Merge shift duration and break flag with df_ta
        merged_df = df_ta.merge(shifts, how='left', on=['emp_id', 'shift_date'])

        # print(results) # test for faulty date

        # Append row-shifted columns 'emp_id', 'shift_date' and 'shift_start_time' to calculate break duration
        merged_df['next_emp_id'] = merged_df['emp_id'].shift(periods=-1).fillna('')  # collect next row's emp_id
        merged_df['next_shift_date'] = merged_df['shift_date'].shift(periods=-1).fillna(
            pd.to_datetime('1900-01-01 00:00:00'))  # collect next rows' shift date
        merged_df['next_shift_start_time'] = merged_df['shift_start'].shift(periods=-1).fillna(
            pd.to_datetime('1900-01-01 00:00:00'))  # collect next row's start time

        # Check using above if the next row is part of the same shift (same emp id and shift start date)
        merged_df['next_shift_flag'] = 0
        merged_df.loc[(merged_df['emp_id'] == merged_df['next_emp_id']) &
                      (merged_df['shift_date'] == merged_df['next_shift_date']),
                      'next_shift_flag'] = 1  # flag if same shift

        merged_df['next_shift_start_time'] = pd.to_datetime(merged_df['next_shift_start_time'])
        merged_df['shift_end'] = pd.to_datetime(merged_df['shift_end'])

        # If both id and shift match, then calculate timedelta
        merged_df.loc[merged_df['next_shift_flag'] == 1, 'break_dur'] = merged_df['next_shift_start_time'] - \
                                                                          merged_df['shift_end']
        merged_df.loc[merged_df['break_dur'].isnull(), 'break_dur'] = pd.to_timedelta(
            0)  # replace null with 0 timedelta

        # convert timedelta to integer minutes
        merged_df['break_dur'] = merged_df['break_dur'].dt.total_seconds().div(60).astype(int)

        # generate list of shifts where employee did not take entitled break, or break taken is less than 30 min
        merged_df['break_not_taken'] = 0
        merged_df.loc[(merged_df['break_flag'] == 1) & (merged_df['next_shift_flag']) &
                      (merged_df['break_dur'] < min_break_duration_mins), 'break_not_taken'] = 1

        return merged_df.to_dict(orient='list')
    
    # Ollie
    def test_8(self):
        """Time & attendance: Minimum and maximum engagement periods - Analyse average hours worked on daily, weekly \
        and fortnightly basis to identify potential non-compliance."""

        df_ta = self.df_ta.__deepcopy__()

        # Calculating the length of the shift in minutes
        df_ta['shift_len_mins'] = (df_ta['shift_end'] - df_ta['shift_start']).dt. \
            total_seconds().div(60).astype(int)

        # Creating a dataframe with just unique emp_id
        results = pd.DataFrame(df_ta['emp_id'].unique(), columns=['emp_id'])
        # 'D' Calculates the sum of the minutes worked for each employee for each day.
        # 'W' Calculates the sum of the minutes worked for each employee for each week. The week starts on Monday and\
        # runs to Sunday. The given shift_date in the below table is the last dat of that week.
        # 'SM' Calculates the sum of the minutes worked for each employee for each fortnight. The fortnight starts on \
        # the 1st of each month and runs to the 14th of the month. The shift_date given is the start of the fortnight.
        freq_list = ['D', 'W', 'SM']
        # Looping over the frequencies in the freq_list
        for freq in freq_list:
            # Getting the mean hours worked for each employee for each frequency
            example = df_ta.groupby(['emp_id', pd.Grouper(key='shift_start', freq=freq)])['shift_len_mins'] \
                          .sum().reset_index().sort_values('shift_start').groupby(['emp_id']).mean() / 60
            # Saving the results to the results table
            results = results.merge(example, left_on='emp_id', right_on='emp_id')
        # renaming the results table
        results.columns = ['emp_id', 'daily_ave_hr', 'weekly_ave_hr', 'fortnightly_ave_hr']

        # R Rounding the results to 2DP
        results['daily_ave_hr'] = round(results['daily_ave_hr'], 2)
        results['weekly_ave_hr'] = round(results['weekly_ave_hr'], 2)
        results['fortnightly_ave_hr'] = round(results['fortnightly_ave_hr'], 2)

        return results.to_dict(orient='list')

    # Jack
    def test_9(self, min_gap):
        """Time & attendance: Gaps between shifts - Analyse gaps between shifts to identify potential non-compliance \
        if not paid at the correct rate."""

        # difference between tsid_act_end_time and tsid_act_start_time of the next shift
        # Looking for employee needing to have certain length of break between shifts
        # Parameter: Minimum amount of time off between shifts required (for example 10)

        # output - Employees who breach this / all occurrences TEST
        df_ta = self.df_ta.__deepcopy__()

        # Sort by emp_id, shift date, shift start time
        df_ta = df_ta.sort_values(by=['emp_id', 'shift_start'], ascending=True)

        # Creating the shift_date column for anlysis
        df_ta['shift_date'] = df_ta['shift_start'].dt.date.apply(lambda x: x.strftime('%Y-%m-%d'))

        # Get shift start and end time for each employee on each day --- CHECK IF THIS IS REDUNDANT?
        shifts = df_ta.groupby(['emp_id', 'shift_date']).agg({'shift_start': 'min', 'shift_end': 'max'})
        shifts.columns = ['min_shift_start', 'max_shift_end']
        shifts = shifts.reset_index()

        # Append row-shifted columns 'emp_id', 'shift_date' and 'min_shift_start_time' to calculate break between shifts
        shifts['next_emp_id'] = shifts['emp_id'].shift(periods=-1).fillna('')  # collect next row's emp_id
        shifts['next_shift_date'] = shifts['shift_date'].shift(periods=-1).fillna(
            pd.to_datetime('1900-01-01 00:00:00'))  # collect next rows' shift date
        shifts['next_shift_start'] = shifts['min_shift_start'].shift(periods=-1).fillna(
            pd.to_datetime('1900-01-01 00:00:00'))  # collect next row's start time

        shifts['next_shift_start'] = pd.to_datetime(shifts['next_shift_start'])

        shifts['max_shift_end'] = pd.to_datetime(shifts['max_shift_end'])

        # Calculate timedelta
        shifts.loc[shifts['emp_id'] == shifts['next_emp_id'], 'shift_gap'] = shifts['next_shift_start'] - shifts[
            'max_shift_end']
        shifts.loc[shifts['shift_gap'].isnull(), 'shift_gap'] = pd.to_timedelta(0)  # replace null with 0 timedelta

        # Convert timedelta to float hours
        shifts['shift_gap'] = shifts['shift_gap'].dt.total_seconds().div(3600)

        # Flag if employee was entitled to a break (shift gap < 10 hours)
        shifts['min_gap_not_taken'] = 0
        shifts.loc[(shifts['shift_gap'] > 0.0) & (shifts['shift_gap'] < min_gap), 'min_gap_not_taken'] = 1

        return shifts.to_dict(orient='list')

    # Philip
    def test_10(self):
        """Time & attendance: Consecutive shifts - Analyse number of consecutive shifts worked to identify potential \
        non-compliance if not paid at the correct rate."""
        df_ta = self.df_ta.__deepcopy__()

        # Creating the shift_date column for anlysis
        df_ta['shift_date'] = df_ta['shift_start'].dt.date.apply(lambda x: pd.to_datetime(x))

        s = df_ta.groupby('emp_id').shift_date.diff().dt.days.ne(1).cumsum()
        results = df_ta.groupby(['emp_id', s]).size().reset_index().rename({0: 'count_of_consecutive_days_worked'},
                                                                           axis='columns')

        results = results.loc[results['count_of_consecutive_days_worked'] > 1]

        results = results.drop(['shift_date'], axis=1)

        return results.to_dict(orient='list')

    def main(self):
        pass
