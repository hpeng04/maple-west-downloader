from datetime import datetime, timedelta
from log import Log
import pandas as pd
from color import color

'''
Log error format:
Unit {unit_no}: {date}, Index {index}: {error_message}
'''

# Function to increment a given time string by a specified number of minutes
def increment_time(time: str, minutes: int = 1) -> str:
    time_format = "%Y-%m-%d %H:%M:%S"
    time_obj = datetime.strptime(time, time_format)
    new_time_obj = time_obj + timedelta(minutes=minutes)
    return str(new_time_obj.strftime(time_format))

# Function to find the time step between two time strings and log the type of data (minute or hourly)
def find_time_step(initial_time: str, second_time: str, unit_no) -> int:
    time_format = "%Y-%m-%d %H:%M:%S"
    initial_time_obj = datetime.strptime(initial_time, time_format)
    second_time_obj = datetime.strptime(second_time, time_format)
    time_diff = second_time_obj - initial_time_obj
    
    # Convert time difference to minutes
    time_step = int(time_diff.total_seconds() / 60)

    # Log and print the type of data based on the time step
    if time_step == 1:
        pass
        # Log.write(f'Unit {unit_no}: Minute data detected')
        # print(f"Unit {unit_no}: Minute data detected")
    elif time_step == 60:
        pass
        # Log.write(f'Unit {unit_no}: Hourly data detected')
        # print(f"Unit {unit_no}: Hourly data detected")
    elif time_step == -1:
        Log.write(f'Unit {unit_no}: Data order is reversed')
        print(f"{color.YELLOW}Unit {unit_no}: Data order is reversed{color.END}")
        time_step = -1
    elif time_step == -60:
        pass
        # Log.write(f'Unit {unit_no}: Data order is reversed, Hourly data detected')
        # print(f"{color.YELLOW}Unit {unit_no}: Data order is reversed, Hourly data detected{color.END}")
        # time_step = -60
    else:
        Log.write(f'Unit {unit_no}: Time step could not be determined')
        print(f"{color.YELLOW}Unit {unit_no}: Time step could not be determined{color.END}")
        time_step = 1
    
    return time_step

# Function to check for missing rows in a DataFrame and log errors
def check_missing_rows(data: pd.DataFrame, unit_no) -> pd.DataFrame:
    if data is None:
        return None, [], [], []
    errors = []
    warnings = []
    bad_indices = []

    first_row = data.iloc[0]
    num_columns = len(first_row)

    initial_time = data.iloc[0, 0]
    second_row_time = data.iloc[1, 0]
    time_step = find_time_step(initial_time, second_row_time, unit_no)

    final_time = data.iloc[-1, 0]
    
    index = 0
    current_time = data.iloc[index, 0].split(" ")[0] + " 00:00:00"
    expected_time = current_time

    while current_time != final_time:
        counter = 0
        expected_time = increment_time(expected_time, time_step)
        index += 1
        current_time = data.iloc[index, 0]

        # Check for missing rows and log errors
        while current_time != expected_time:
            counter += 1

            current_time_obj = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")
            expected_time_obj = datetime.strptime(expected_time, "%Y-%m-%d %H:%M:%S")

            if current_time_obj < expected_time_obj:
                if time_step != 60:
                    Log.write(f'Unit {unit_no}: Data order error at index {index}')
                    print(f"{color.RED}Unit {unit_no}: Data order error at index {index}{color.END}")
                    errors.append(f"Unit {unit_no}: Data order error at index {index}")
                    bad_indices.append(index)
                    expected_time = str(current_time)
                break
            
            if time_step != 60:
                Log.write(f'Unit {unit_no}: {expected_time} Index {index}: Missing all data')
            # print(f"{color.RED}Unit {unit_no}: {expected_time} Index {index}: Missing all data{color.END}")
            if counter > 4:
                errors.append(f"Unit {unit_no}: {expected_time} Index {index}: Missing all data")
            else: 
                warnings.append(f"Unit {unit_no}: {expected_time} Index {index}: Missing all data")
            bad_indices.append(index)
            missing_row = [expected_time] + [""] * (num_columns - 1)
            data = pd.concat([data.iloc[:index], pd.DataFrame([missing_row], columns=data.columns), data.iloc[index:]]).reset_index(drop=True)
            expected_time = increment_time(expected_time, time_step)
            index += 1  # Adjust index to account for the inserted row

    return data, errors, warnings, bad_indices

def check_total_energy(data, unit_no):
    errors = []
    warnings = []


    # iterate through data rows
    for index, row in data.iterrows():
        main_elec_cols = row.filter(regex='Main\\s*Electricity(?!\\s*Gen).*(Watts)$')
        pv_cols = row.filter(regex="PV.*(Watts)$")
        energy_generated = pd.to_numeric(main_elec_cols).sum(skipna=True) + pd.to_numeric(pv_cols).sum(skipna=True)        
        elec_cols = row.filter(regex="^(?!.*Gen\\s).*(Watts)$")
        energy_consumed = pd.to_numeric(elec_cols).sum(skipna=True) - energy_generated

        # Round the energy values to 2 decimal places
        energy_generated = round(energy_generated, 2)
        energy_consumed = round(energy_consumed, 2)

        diff = energy_generated - energy_consumed
        if energy_generated*1.05 >= energy_consumed or abs(diff) < 10: # 1% tolerance
            pass
        else: # 5% tolerance
            Log.write(f"Unit {unit_no}: {data.iloc[index, 0]} Index {index}: Energy Consumed: {energy_consumed} > Energy Generated: {energy_generated}")
            print(f"{color.YELLOW}Unit {unit_no}: {data.iloc[index, 0]} Index {index}: Energy Consumed: {energy_consumed} > Energy Generated: {energy_generated}{color.END}")
            errors.append(f"{data.iloc[index, 0]} Index {index}: Energy Consumed: {energy_consumed} > Energy Generated: {energy_generated}")
        # else:
        #     Log.write(f"Unit {unit_no}: {data.iloc[index, 0]} Index {index}: Energy Consumed: {energy_consumed} > Energy Generated: {energy_generated}")
        #     print(f"{color.RED}Unit {unit_no}: {data.iloc[index, 0]} Index {index}: Energy Consumed: {energy_consumed} > Energy Generated: {energy_generated}{color.END}")
        #     errors.append(f"{data.iloc[index, 0]} Index {index}: Energy Consumed: {energy_consumed} > Energy Generated: {energy_generated}")
    return errors, warnings

# Function to check if values in a DataFrame column are within specified limits and log errors
def check_limits(regex, data, min_value, max_value, unit_no, bad_indices):
    errors = []
    warnings = []
    try:
        column = data.filter(regex=regex).columns[0]
        values = data[column]
        column_name = column.lstrip("0123456789- ")
        null_counter = 0
        limit_counter = 0
        for index, value in values.items():
            if index in bad_indices:
                continue
            if value == None or pd.isna(value) or value == "":  # Skip empty values
                null_counter += 1
                # print(f"{color.YELLOW}Unit {unit_no}: {data.iloc[index, 0]} Index {index}: Missing data in {column_name}{color.END}")
                Log.write(f"Unit {unit_no}: {data.iloc[index, 0]} Index {index}: Missing data in {column_name}")
                if null_counter > 10:
                    errors.append(f"{data.iloc[index, 0]} Multiple missing data in {column_name}")
                else:
                    warnings.append(f"{data.iloc[index, 0]} Index {index}: Missing data in {column_name}")
            elif float(value) < min_value or float(value) > max_value:
                limit_counter += 1
                # print(f"{color.YELLOW}Unit {unit_no}: {data.iloc[index, 0]} Index {index}: {column_name} out of limits, Value: {value}, Limits: ({min_value}, {max_value}){color.END}")
                Log.write(f"Unit {unit_no}: {data.iloc[index, 0]} Index {index}: {column_name} out of limits, Value: {value}, Limits: ({min_value}, {max_value})")
                if limit_counter > 2:
                    errors.append(f"{data.iloc[index, 0]} Index {index}: {column_name} out of limits, Value: {value}, Limits: ({min_value}, {max_value})")
                else:
                    warnings.append(f"{data.iloc[index, 0]} Index {index}: {column_name} out of limits, Value: {value}, Limits: ({min_value}, {max_value})")
            else:
                null_counter = 0
                limit_counter = 0
        return errors, warnings
    except IndexError:
        print(f"{color.RED}Unit {unit_no}: Column not found: {regex}{color.END}")
        Log.write(f"***Unit {unit_no}: Column not found: {regex}")
        errors.append(f"Column not found: {regex}")
        return errors, warnings

def check_activity(regex, data, unit_no):
    errors, warnings = [], []
    column = data.filter(regex=regex).columns[0]
    column_name = column.lstrip("0123456789- ")
    if pd.to_numeric(data[column]).sum(skipna=True) == 0:
        print(f"{color.YELLOW}Unit {unit_no}: {column_name} no response - Possible Disconnection{color.END}")
        Log.write(f"Unit {unit_no}: {column_name} no response - Possible Disconnection")
        errors.append(f"{column_name} no response - Possible Disconnection")
    return errors, warnings

def check_pulse(regex, data, min_value, max_value, unit_no, bad_indices):
    errors, warnings = check_limits(regex, data, min_value, max_value, unit_no, bad_indices)
    if f"Unit {unit_no}: Column not found: {regex}" in errors:
        return errors, warnings
    column = data.filter(regex=regex).columns[0]
    column_name = column.lstrip("0123456789- ")
    if pd.to_numeric(data[column]).sum(skipna=True) == 0:
        print(f"{color.YELLOW}Unit {unit_no}: {column_name} no response - Possible Disconnection{color.END}")
        Log.write(f"Unit {unit_no}: {column_name} no response - Possible Disconnection")
        errors.append(f"{column_name} no response - Possible Disconnection")
    return errors, warnings

def find_diff(regex ,data, unit_no, bad_indices):
    column = data.filter(regex=regex).columns[0]
    values = data[column]
    start_index = 0
    while pd.isna(values[start_index]) or values[start_index] == "":
        start_index += 1
    min_value = values[start_index]
    max_value = values[start_index]
    for index, value in values.items():
        if index in bad_indices:
            continue
        if value == None or pd.isna(value) or value == "":  # Skip empty values
            continue
        if float(value) < min_value:
            min_value = float(value)
        if float(value) > max_value:
            max_value = float(value)
    diff = abs(max_value - min_value)
    return min_value, max_value, diff


def check_water_pulse(regex, data, min_limit, max_limit, unit_no, bad_indices):
    errors, warnings = check_limits(regex, data, min_limit, max_limit, unit_no, bad_indices)
    if f"Unit {unit_no}: Column not found: {regex}" in errors:
        return errors, warnings
    min_value, max_value, diff = find_diff(regex, data, unit_no, bad_indices)
    if diff > 5:
        activity_errors, activity_warnings = check_activity(regex, data, unit_no)
        errors += activity_errors
        warnings += activity_warnings
    return errors, warnings