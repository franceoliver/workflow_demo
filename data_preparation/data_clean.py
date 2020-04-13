import pandas as pd
import datetime as dt
import os
from pandasql import sqldf


# Initial Data cleaning of the TA data required.
def df_ta_clean(df_ta):
#     df_ta = df_ta.filter(['emp_mstid', 'tsid_start_date', 'tsid_act_start_time', 'tsid_act_end_time'], axis=1)
#     df_ta.rename(columns={'emp_mstid': 'emp_id',
#                           'tsid_start_date': 'shift_date',
#                           'tsid_act_start_time': 'shift_start_time',
#                           'tsid_act_end_time': 'shift_end_time'}, inplace=True)
#     # list of columns we need to reformat to datetime
#     times = ['shift_start_time',
#              'shift_end_time',
#              ]
#     # Reformatting listed columns to datetime
#     for x in times:
#         try:
#             df_ta[x] = pd.to_datetime(df_ta[x], yearfirst=True)
#         except:
#             print('Cannot convert', x)
#             pass
    df_ta = db_obj.retrieve_data('raw_rda_dummy_data_ta')
    df_ta = df_ta.filter(['emp_mstid','tsid_act_start_time', 'tsid_act_end_time'], axis=1)

    #mapping the columns you want to keep
    df_ta.rename(columns={'emp_mstid': 'emp_id',
                          'tsid_act_start_time': 'shift_start',
                          'tsid_act_end_time': 'shift_end'}, inplace=True)

    # list of columns we need to reformat to datetime
    times = ['shift_start','shift_end']

    # Reformatting listed columns to datetime
    for x in times:
        df_ta[x] = pd.to_datetime(df_ta[x], yearfirst=True)

    # Remove duplicated rows
    df_ta = df_ta.drop_duplicates()

    #save df_ta to cln_shift
    db_obj.insert_data(df_ta, 'cln_shift')

    return df_ta


def df_master_clean(df_master):
    # Bringing in the levels data to the masters data
    level_map = pd.read_csv(os.path.join("data", "level_mapping_key", "ref_role_level_map.csv"))
    # Merging them together
    df_master = df_master.merge(level_map[['Location_Position', 'Level']], how='left', left_on='Position',
                                right_on='Location_Position')

    # Renaming inline with our expectations
    df_master.rename(columns={'Staff Member': 'emp_id',
                              'Title': 'title',
                              'First Name': 'first_name',
                              'Surname': 'surname',
                              'Birth Date': 'dob',
                              'Joined': 'date_joined',
                              'Last Day of Duty': 'last_day_of_duty',
                              'UPDATED Last Day of Duty': 'updated_last_day_of_duty',
                              'Hours/Week': 'hours_per_week',
                              'venue_number': 'venue_number',
                              'Venue': 'venue',
                              'Position Start': 'position_start_date',
                              'Position End': 'position_end_date',
                              'position_number': 'position_number',
                              'Position': 'position_name',
                              'Employment Status': 'employment_status',
                              'Annual': 'annual',
                              'Hourly': 'hourly',
                              'Company': 'company',
                              'Cost Centr': 'cost_centre',
                              'Location_Position': 'location_position',
                              'Level': 'level'}, inplace=True)

    # listing date columns
    date_columns = ['dob', 'date_joined', 'last_day_of_duty', 'position_start_date', 'position_end_date']

    # changing said columns to datetime
    for date in date_columns:
        df_master[date] = pd.to_datetime(df_master[date], dayfirst=True)

    # Function to iterate over the dataframe and turn all emp_ids into intergers
    # this is need on a line by line basis as some of these have a letter in front of them!
    def emp_id_format(row):
        try:
            return int(row)
        except:
            return row

    df_master['emp_id'] = df_master['emp_id'].apply(emp_id_format)

    # This is a specific to rockpool issue.
    # Rockpool data has multiple entries for the emp_id as if exisiting employees start a new role they remain in the
    # master data.
    # However I their current role is always the most recent in the data so if we remove duplciate but keep the first
    # we avoid duplication or mix up of totals within roles.
    df_master.drop_duplicates(keep='first', subset='emp_id', inplace=True)

    # Function to rename the employee position based off key words.
    def base_position(row):
        row = str(row)
        if 'Acting General Manager' in row:
            return 'Acting General Manager'
        elif 'Acting Head Chef' in row:
            return 'Acting Head Chef'
        elif 'Acting Venue Manager' in row:
            return 'Acting Venue Manager'
        elif 'Assistant Head Chef' in row:
            return 'Assistant Head Chef'
        elif 'Assistant Head Sommelier' in row:
            return 'Assistant Head Sommelier'
        elif 'Assistant Head Sommellier' in row:
            return 'Assistant Head Sommelier'
        elif 'row.str.contains(''Assistant Sommelier' in row:
            return 'Assistant Sommelier'
        elif 'Assistant Sommellier' in row:
            return 'Assistant Sommelier'
        elif 'Assistant Restaurant Manager' in row:
            return 'Assistant Restaurant Manager'
        elif 'BOH Store Manager' in row:
            return 'BOH Store Manager'
        elif 'Host Supervisor' in row:
            return 'Host Supervisor'
        elif 'National Bar Manager' in row:
            return 'National Bar Manager'
        elif 'Senior Bartender' in row:
            return 'Senior Bartender'
        elif 'Pastry Sous Chef' in row:
            return 'Pastry Sous Chef'
        elif 'Pastry Chef De Partie' in row:
            return 'Pastry Chef De Partie'
        elif 'Senior Chef De Partie' in row:
            return 'Senior Chef De Partie'
        elif 'Senior Sous Chef' in row:
            return 'Senior Sous Chef'
        elif 'Group Sommelier' in row:
            return 'Group Sommelier'
        elif 'Senior Chef' in row:
            return 'Senior Chef'
        elif 'row.str.contains(''Bar Manager' in row:
            return 'Bar Manager'
        elif 'Bar Supervisor' in row:
            return 'Bar Supervisor'
        elif 'Bartender' in row:
            return 'Bartender'
        elif 'Chef/Admin' in row:
            return 'Chef/Admin'
        elif 'Commis Chef' in row:
            return 'Commis Chef'
        elif 'Demi Chef' in row:
            return 'Demi Chef'
        elif 'Demi Pastry Chef' in row:
            return 'Demi Pastry Chef'
        elif 'Dumpling Chef' in row:
            return 'Dumpling Chef'
        elif 'Head Butcher' in row:
            return 'Head Butcher'
        elif 'Butcher' in row:
            return 'Butcher'
        elif 'Head Pastry' in row:
            return 'Head Pastry Chef'
        elif 'Executive Sushi Chef' in row:
            return 'Executive Sushi Chef'
        elif 'Executive Chef' in row:
            return 'Executive Chef'
        elif 'Corporate Pastry Chef' in row:
            return 'Corporate Pastry Chef'
        elif 'Sushi Chef' in row:
            return 'Sushi Chef'
        elif 'Polisher' in row:
            return 'Polisher'
        elif 'Sous Chef Pastry' in row:
            return 'Sous Chef Pastry'
        elif 'Sushi Chef' in row:
            return 'Sushi Chef'
        elif 'Brand Chef' in row:
            return 'Brand Chef'
        elif 'Senior Sushi' in row:
            return 'Senior Sushi Chef'
        elif 'Storeman' in row:
            return 'Storeman'
        elif 'Chef De Partie' in row:
            return 'Chef De Partie'
        elif 'Chef de Partie' in row:
            return 'Chef De Partie'
        elif 'Pizza Chef' in row:
            return 'Pizza Chef'
        elif 'Cook' in row:
            return 'Cook'
        elif 'General Manager' in row:
            return 'General Manager'
        elif 'General Manger' in row:
            return 'General Manager'
        elif 'Head Host' in row:
            return 'Head Host'
        elif 'Sommelier Director' in row:
            return 'Sommelier Director'
        elif 'Head Sommelier' in row:
            return 'Head Sommelier'
        elif 'Head Waiter' in row:
            return 'Head Waiter'
        elif 'Host' in row:
            return 'Host'
        elif 'Pasta Chef' in row:
            return 'Pasta Chef'
        elif 'Kitchenhand' in row:
            return 'Kitchenhand'
        elif 'Restaurant Manager' in row:
            return 'Restaurant Manager'
        elif 'Restaurant Supervisor' in row:
            return 'Restaurant Supervisor'
        elif 'Senior Waiter' in row:
            return 'Senior Waiter'
        elif 'Store Manager' in row:
            return 'Store Manager'
        elif 'Supervisor' in row:
            return 'Supervisor'
        elif 'Team Co-Ordinator' in row:
            return 'Team Co-Ordinator'
        elif 'Team Co-ordinator' in row:
            return 'Team Co-Ordinator'
        elif 'Team Leader' in row:
            return 'Team Leader'
        elif 'Team Manager' in row:
            return 'Team Manager'
        elif 'Team Member' in row:
            return 'Team Member'
        elif 'Venue Manager' in row:
            return 'Venue Manager'
        elif 'Barman' in row:
            return 'Barman'
        elif 'Beverage Director' in row:
            return 'Beverage Director'
        elif 'Pastry Chef' in row:
            return 'Pastry Chef'
        elif 'Head Chef' in row:
            return 'Head Chef'
        elif 'Jnr Sous' in row:
            return 'Jnr Sous Chef'
        elif 'Sous Chef' in row:
            return 'Sous Chef'
        elif 'Sommelier' in row:
            return 'Sommelier'
        elif 'Sushi Chef' in row:
            return 'Sushi Chef'
        elif 'Chef' in row:
            return 'Chef'
        elif 'Waiter' in row:
            return 'Waiter'
        elif 'Food Runner' in row:
            return 'Food Runner'
        elif 'Area Manager' in row:
            return 'Area Manager'
        elif 'Reservation Coordinator' in row:
            return 'Reservation Coordinator'
        elif 'Assistant Bar Manager' in row:
            return 'Assistant Bar Manager'
        elif 'Reservations & Events Manager' in row:
            return 'Reservations & Events Manager'
        elif 'Reservation & Admin Coordinator' in row:
            return 'Reservation & Admin Coordinator'
        elif 'Bar Manager' in row:
            return 'Bar Manager'
        elif 'Assistant Restaurant Manger' in row:
            return 'Assistant Restaurant Manger'
        else:
            return row

    df_master['position_name'] = df_master['position_name'].apply(base_position)

    return df_master


# Initial Data cleaning of the PA data required.
def df_pa_clean(df_pa):
    # Renaming the raw data columns in line with standards for the tests
    df_pa.rename(columns={'detnumber': 'emp_id',
                          'pitrunno': 'pay_cycle',
                          'pitrundate': 'payment_date',
                          'pitdedalw': 'pitdedalw',
                          'pitcode': 'pay_type',
                          'pitamount': 'period_amount',
                          'pithours': 'hours_for_period',
                          'posstatus': 'position_status'}, inplace=True)

    # converting date field to datetime
    df_pa['payment_date'] = pd.to_datetime(df_pa['payment_date'], dayfirst=True)

    # bringing in a mapping key as provided by the rockpool pwc team (jack and phil)
    payroll_mapping_key = pd.read_csv(os.path.join("data", "payroll_mapping_key", "payroll_data_key.csv"))
    df_pa = df_pa.merge(payroll_mapping_key, left_on='pay_type', right_on='mapping_codes', how='left')

    # Bringing in masterData to add position data onto to transaction date.
    df_master = df_master_clean(pd.read_csv(os.path.join("data", "Employee Data UPDATED.csv")))

    # Filtering the data set on specific values needed for clac.
    df_master_filtered = df_master.filter(items=['emp_id', 'position_start_date', 'position_end_date', 'position_name', \
                                                 'level', 'employment_status', 'venue'])
    # Creating a new column based on emp_id and role
    df_master_filtered['emp_id_role'] = df_master_filtered['emp_id'].map(str) + '_' + df_master_filtered[
        'position_name']
    # removing duplciates on the joint emp_id and role column as this means role is consitent
    df_master_filtered.drop_duplicates(subset='emp_id_role', inplace=True)
    # Removing all NaT values in teh position start date
    df_master_filtered = df_master_filtered[~df_master_filtered['position_start_date'].isnull()]
    # replacing the NaT's in current roles with the max date of payments in pa data so as to set the correct range of dates.
    max = df_pa['payment_date'].max()
    df_master_filtered['position_end_date'] = df_master_filtered['position_end_date'].fillna(max)

    q = '''
    select a.*, b.emp_id_role, b.position_name, b.level, b.employment_status, b.venue
    from df_pa a
    left join df_master_filtered b
    on a.emp_id = b.emp_id
    and a.payment_date >= b.position_start_date and a.payment_date <= b.position_end_date
    '''

    pysqldf = sqldf(q, locals())

    return pysqldf


def unique_values_dataframe(df):
    uniques_df = pd.DataFrame()
    for x in df.columns:
        temp_df = pd.DataFrame(df[x].unique()).rename(columns={0: x})
        uniques_df = pd.concat([uniques_df, temp_df], axis=1)
    return uniques_df


# Assigning a AM / PM tag to the data set
def am_or_pm(row):
    if row.time() < dt.time(12):
        return 'AM'
    else:
        return 'PM'


# Assigning a day tag to the data.
def str_dow(row):
    if row == 0:
        return 'Mon'
    elif row == 1:
        return 'Tue'
    elif row == 2:
        return 'Wed'
    elif row == 3:
        return 'Thur'
    elif row == 4:
        return 'Fri'
    elif row == 5:
        return 'Sat'
    elif row == 6:
        return 'Sun'


# calling the number of breaches within the function
def breaches(row):
    return len(row)


# Removing duplicates form a list
def Remove(duplicate):
    final_list = []
    for num in duplicate:
        if num not in final_list:
            final_list.append(num)
    return final_list


def stringing(row):
    try:
        return str(row)
    except:
        return row


def interging(row):
    try:
        return int(row)
    except:
        return row
