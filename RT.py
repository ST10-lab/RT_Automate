import pandas as pd
import yaml
import os
from simple_salesforce import SalesforceLogin, Salesforce

def read_delta_file(delta_file_path):
    delta_df = pd.read_csv(delta_file_path)
    return delta_df   
def read_config(file_path='config.yml'):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config
def run_soql_query(sf, query):
    result = sf.query_all(query)
    records = result['records']
    return records


#delta_file_path = 'D:\IntegrationJobs\WorkDayInbound\tmp\workdayinbound_2024-07-09-02-30-08'
delta_file_path = 'Base Directory/Delta_File/SalesIQ_Employee_Delta_File_20240715.txt'
delta_df= pd.read_csv(delta_file_path, delimiter = '|')
delta_df

BU_names = ['OncHSC', 'OncB', 'OncL', 'OncSC', 'OncSC II']
product_mappings = {
    'OncHSC': ['Vanflyta', 'Injectafer'],
    'OncB': 'ENHERTU BC',
    'OncL': 'ENHERTU LC',
    'OncSC': 'Injectafer',
    'OncSC II': 'Injectafer'
}


Position_type_df_1 = pd.read_csv('Base Directory/Current_day_Backup/AxtriaSalesIQTM__Position__c_071524.csv')
employee_history_df = pd.read_csv('Base Directory/Current_day_Backup/Employee_History__c_071524.csv')
employee_details_df = pd.read_csv('Base Directory/Current_day_Backup/AxtriaSalesIQTM__Employee__c_071524.csv')
employee_details_Master_df = pd.read_csv('Base Directory/Current_day_Backup/AxtriaSalesIQST__SIQ_Employee_Master__c_071524.csv',encoding='latin1')
#leave_journey_df = pd.read_csv('./share to sai 1/Backups/20240118_033502/Leave_Journey__c030724.csv')
employee_feed_df = pd.read_csv('Base Directory/Current_day_Backup/AxtriaSalesIQST__CR_Employee_Feed__c_071524.csv')
Time_Type_Journey_df = pd.read_csv('Base Directory/Current_day_Backup/Time_Type_Jounery__c_071524.csv')
Training_Journey_df = pd.read_csv('Base Directory/Current_day_Backup/Training_Journey__c_071524.csv')
Position_type_df =pd.read_csv('Base Directory/Current_day_Backup/AxtriaSalesIQTM__Position__c_071524.csv')
#employee_IC_history_df=pd.read_csv('./share to sai 1/Backups/20240118_033502/Employee_IC_History__c030724.csv')
Position_type_df_Previous= pd.read_csv('Base Directory/Previous_day_Backup/AxtriaSalesIQTM__Position__c_071424.csv')#.astype(str)
employee_details_df_Previous= pd.read_csv('Base Directory/Previous_day_Backup/AxtriaSalesIQTM__Employee__c_071424.csv')
employee_IC_history_df=pd.read_csv('Base Directory/Current_day_Backup/Employee_IC_History__c_071524.csv')
employee_history_df_Prev = pd.read_csv('Base Directory/Previous_day_Backup/Employee_History__c_071424.csv')
employee_position =  pd.read_csv('Base Directory/Current_day_Backup/AxtriaSalesIQTM__Position_Employee__c_071524.csv')
employee_position_Prev =  pd.read_csv('Base Directory/Previous_day_Backup/AxtriaSalesIQTM__Position_Employee__c_071424.csv')
Position_type_df_1['Position_Code__c']= Position_type_df_1['Position_Code__c'].astype(str).str.split('.').str.get(0)

Position_type_df_1['AxtriaSalesIQTM__Effective_Start_Date__c'] = pd.to_datetime(Position_type_df_1['AxtriaSalesIQTM__Effective_Start_Date__c'])
max_date_indices = Position_type_df_1.groupby('Position_Code__c')['AxtriaSalesIQTM__Effective_Start_Date__c'].idxmax()
#latest_date = Position_type_df_1['AxtriaSalesIQTM__Effective_Start_Date__c'].max()
filtered_df = Position_type_df_1.loc[max_date_indices]

employee_details_df['AxtriaSalesIQTM__Employee_ID__c']= employee_details_df['AxtriaSalesIQTM__Employee_ID__c'].astype(str)

delta_df['Position_Code'] =delta_df['Position_Code'].astype(str).str.split('.').str.get(0)

import pandas as pd

def extract_BU_classification(delta_df, Position_type_df_1, BU_names, product_mappings):
    
    # Convert Position_Code type to match Position_Code__c dtype
    delta_df['Position_Code'] = delta_df['Position_Code'].astype(Position_type_df_1['Position_Code__c'].dtype)
    
    # Merge the DataFrames on Position_Code__c and Position_Code
    merged_df = pd.merge(Position_type_df_1, delta_df, left_on='Position_Code__c', right_on='Position_Code', how='right')
    
    # Extract Bu_Name column
    Bu_Name = merged_df['SalesforceCode__c']
    
    # Create a DataFrame from the product mappings dictionary
    merged_df['flag_BU_classification'] = merged_df.apply(lambda ele:'Sales' if ele['SalesforceCode__c'] in BU_names else 'Non-sales',axis=1)
    df_products = pd.DataFrame(product_mappings.items(), columns=['BU', 'Product'])
    df_products = df_products.explode('Product')
    
    
    # Merge the merged DataFrame with the product DataFrame
    BU_product_added_df = merged_df.merge(df_products, left_on='SalesforceCode__c', right_on='BU', how='left')
    
    # Filter and get unique products for Sales classification
    unique_products = BU_product_added_df[BU_product_added_df['flag_BU_classification'] == 'Sales']['Product'].unique()
    
    # Extend the delta_df columns with new columns
    delta_df_columns = list(delta_df.columns)
    delta_df_columns.extend(['BU', 'Product', 'flag_BU_classification'])
    
    # Create the final DataFrame with the extended columns and drop duplicates
    delta_df_with_BU_product = BU_product_added_df[delta_df_columns].drop_duplicates(keep='first')
    
    
    return delta_df_with_BU_product




delta_df_with_BU_product = extract_BU_classification(delta_df, filtered_df, BU_names, product_mappings)



print(delta_df_with_BU_product)

df_DTA_Records= delta_df_with_BU_product[(delta_df_with_BU_product['Action_Type'] == 'DTA')]

df_DTA_Records

# Filter Position_type_df_1 based on the condition AxtriaSalesIQTM__IsMaster__c == True
position_type_master_df = Position_type_df_1[Position_type_df_1['AxtriaSalesIQTM__IsMaster__c'] == True]

# Convert Position_Code__c to string and extract the main part
position_type_master_df['Position_Code__c'] = position_type_master_df['Position_Code__c'].astype(str).str.split('.').str.get(0)
df_DTA_Records['Position_Code'] = df_DTA_Records['Position_Code'].astype(str).str.split('.').str.get(0)

# Merge df_DTA_Records with the filtered Position_type_master_df
merged_df_1 = pd.merge(df_DTA_Records, position_type_master_df, 
                       left_on=['Position_Code'], right_on=['Position_Code__c'], 
                       how='left')

# Select the desired columns
selected_columns = merged_df_1[list(df_DTA_Records.columns) + ['AxtriaSalesIQTM__Client_Position_Code__c'] + ['AxtriaSalesIQTM__Effective_Start_Date__c']]


# Remove duplicate rows from the DataFrame
selected_columns_unique = selected_columns.sort_values(by='AxtriaSalesIQTM__Effective_Start_Date__c', ascending=False)
selected_columns_unique = selected_columns.drop_duplicates(keep='first')

# Print the DataFrame with duplicate rows removed

selected_columns_unique

employee_details_df_Previous['Position_Code__c'] = employee_details_df_Previous['Position_Code__c'].astype(str).str.split('.').str.get(0)

# Iterate through each row in the filtered employee_delta DataFrame
# Create an empty list to store the values for the "type" column
type_values = []

for index, row in selected_columns_unique.iterrows():
    # Get the Employee ID and Position ID from the current row
    employee_id = row['Employee_Id']
    position_id = row['Position_Code']
    territory_id = row['AxtriaSalesIQTM__Client_Position_Code__c']
    action_reason = row['Action_Reason']
    
    # Check if there's a corresponding record in df_DTA_Records with the same Employee ID
    if employee_id in employee_details_df_Previous['AxtriaSalesIQST__Employee_PRID__c'].values:
        # Get the Position ID from df_DTA_Records for the corresponding Employee ID
        df_position_id = employee_details_df_Previous.loc[employee_details_df_Previous['AxtriaSalesIQST__Employee_PRID__c'] == employee_id, 'Position_Code__c'].iloc[0]
        print(df_position_id)
        df_Territory_id =  employee_details_df_Previous.loc[employee_details_df_Previous['AxtriaSalesIQST__Employee_PRID__c'] == employee_id, 'Territory__c'].iloc[0]
        print(df_Territory_id)
        # Compare the Position IDs
        if action_reason != 'Promotion' and position_id != df_position_id and df_Territory_id != territory_id:
            type_values.append("Transfer Position code change")
        elif action_reason != 'Promotion' and position_id == df_position_id and df_Territory_id != territory_id:
            type_values.append("Transfer Territory code change")
        elif action_reason == 'Promotion' and position_id == df_position_id:
            type_values.append("Promotion without transfer")
        elif action_reason == 'Promotion' and position_id != df_position_id and df_Territory_id != territory_id:
            type_values.append("Promotion with Transfer by Position code")
        elif action_reason == 'Promotion' and position_id == df_position_id and df_Territory_id != territory_id:
            type_values.append("Promotion with Transfer by Territory code")
        elif action_reason != 'Promotion' and position_id == df_position_id and df_Territory_id == territory_id:
            type_values.append("Sample case")
        else:
            type_values.append("")  # Append an empty string if no condition matches
    else:
        # No corresponding record found in df_DTA_Records for the Employee ID
        type_values.append("No DTA record found for Employee ID: " + employee_id)

# Add the "type" column to the selected_columns_unique DataFrame
selected_columns_unique['type'] = type_values


selected_columns_unique

df_DTA_Records_Sample= selected_columns_unique[(selected_columns_unique['type'] == 'Sample case')]

df_DTA_Records_Sample

data = ['abc']
condition_1_history_table= pd.DataFrame(data,columns = ['Employee_ID'])
# condition_1_history_table = condition_1_history_table.append(pd.DataFrame(data),ignore_index= True)
condition_1_history_table

data = ['']
condition_hist_table1 = pd.DataFrame(data,columns = ['Employee_ID'])
condition_hist_table1


condition_hist_table1.columns

new = pd.DataFrame(columns = ['Employee_ID', 'Action_type', 'Action_Reason', 'S.No.', 'Test Case',
       'Status', 'Reason'])
new

cond1 = pd.DataFrame(columns = ['Employee_ID', 'Action_type', 'Action_Reason', 'S.No.', 'Test Case',
       'Status', 'Reason']) 

cond1 = pd.DataFrame(columns = ['Employee_ID', 'Action_type', 'Action_Reason', 'S.No.', 'Test Case',
       'Status', 'Reason']) 
data = ['']

### Demographic remaning cases 

import pandas as pd
import numpy as np

# Example DataFrame

# Function to convert ZIP code to integer, ignoring non-numeric characters
def convert_zip_to_int(zip_code):
    # Remove non-numeric characters
    numeric_zip = ''.join(filter(str.isdigit, str(zip_code)))
    # Return as integer if numeric_zip is not empty, else return NaN
    return int(numeric_zip) if numeric_zip else np.nan

# Apply the conversion function to the DataFrame column
employee_details_Master_df['SIQ_Work_Zip_c__c'] = employee_details_Master_df['SIQ_Work_Zip_c__c'].apply(convert_zip_to_int)

####DTA Sample Cases

employee_details_df['AxtriaSalesIQTM__Employee_ID__c'] = employee_details_df['AxtriaSalesIQTM__Employee_ID__c'].astype(int).tolist()

for index, row in df_DTA_Records_Sample.iterrows():
    employee_id = row['Employee_Id']
    row_filled = row.fillna('')
    condition_hist_table1['Employee_ID'] = employee_id
    condition_hist_table1['Action_type'] = row['Action_Type']
    condition_hist_table1['Action_Reason'] = row['Action_Reason']
    condition_hist_table1['S.No.'] = index+1
    
    
################################# Employee details ###################################
    employee_details = employee_details_df[employee_details_df['AxtriaSalesIQTM__Employee_ID__c'] == employee_id]
    employee_details_filled = employee_details.fillna('')
    
    if (employee_details['Work_Address_Line_1__c'] == row['Work_Address_Line_1']).all():
        print(f"Employee {employee_id}: Work Address_1 are fine.")
        condition_hist_table1['Test Case'] = 'Checking whether work address_1 are change or not'
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
        
    else:
        print(f"Employee {employee_id}: Work Address_1 are not fine.")
        condition_hist_table1['Test Case'] = 'Checking whether work address_1 are change or not'
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'Work Address_1 are not matching with the delta file please update it accrodingly'
    cond1 = pd.concat([cond1,condition_hist_table1])
    
    if (employee_details['Work_Address_Line_3__c'] == row['Work_Address_Line_3']).all():
        print(f"Employee {employee_id}: Work Address_3 are fine.")
        condition_hist_table1['Test Case'] = 'Checking whether work address_3 are change or not'
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
        
    else:
        print(f"Employee {employee_id}: Work Address_3 are not fine.")
        condition_hist_table1['Test Case'] = 'Checking whether work address_3 are change or not'
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'Work Address_3 are not matching with the delta file please update it accrodingly'
    cond1 = pd.concat([cond1,condition_hist_table1])
    
   
    if (employee_details_filled['Work_Address_Line_2__c'] == row_filled['Work_Address_Line_2']).all():
        print(f"Employee {employee_id}: Work Address_2 are fine.")
        condition_hist_table1['Test Case'] = 'Checking whether work address_2 are change or not'
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
        
    else:
        print(f"Employee {employee_id}: Work Address_2 are not fine.")
        condition_hist_table1['Test Case'] = 'Checking whether work address_2 are change or not'
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'Work Address_2 are not matching with the delta file please update it accrodingly'
    cond1 = pd.concat([cond1,condition_hist_table1])
    
        
    if (employee_details_filled['Work_City__c'] == row_filled['Work_City']).any() and (employee_details_filled['Work_State__c'] == row_filled['Work_State']).any():
        print(f"Employee {employee_id}: Work city and work state are corrcetly populated.")
        condition_hist_table1['Test Case'] = 'Checking whether Work city and work state are corrcetly populated or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else :
        print(f"Employee {employee_id}: Work city and work state are not correctly populated")
        condition_hist_table1['Test Case'] = 'Checking whether Work city and work state are corrcetly populated or not.'
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'Work city and work state are not corrcetly populated.'
    cond1 = pd.concat([cond1,condition_hist_table1])
    
    if (employee_details['Work_Phone_Number__c'] == row['Work_Phone_Number']).any() and (employee_details['AxtriaSalesIQTM__Cellphone_Number__c'] == row['Cell_Number']).any():
        
        print(f"Employee {employee_id}: The mobile numbers are matching.")
        condition_hist_table1['Test Case'] = 'Checking whether all the mobile numbers are matching with the employee details file '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: The mobile numbers are not matching.")
        condition_hist_table1['Test Case'] = 'Checking whether all the mobile numbers are matching with the employee details file '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The mobile numbers are not matching with the employee details file '
        
    
    if (employee_details_filled['AxtriaSalesIQTM__Email__c'] == row_filled['DSI_Email_Id']).any() and (employee_details_filled['AxtriaSalesIQTM__FirstName__c'] == row_filled['First_Name']).any() and (employee_details_filled['AxtriaSalesIQTM__Middle_Name__c'] == row_filled['Middle_Name']).any() and (employee_details_filled['AxtriaSalesIQTM__Last_Name__c'] == row_filled['Last_Name']).any():
        
        print(f"Employee {employee_id}: All the names and Email id are correct.")
        condition_hist_table1['Test Case'] = 'Checking whether All the names and Email id are correct or not.'
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: All the names and Email id are not correct.")
        condition_hist_table1['Test Case'] = 'Checking whether all the mobile numbers are matching with the employee details file '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = ' All the names and Email id are not correct '
    cond1 = pd.concat([cond1,condition_hist_table1])


employee_details_Master_df['SIQ_Position_Code_c__c']= employee_details_Master_df['SIQ_Position_Code_c__c'].astype(str).str.split('.').str.get(0)
for index, row in df_DTA_Records_Sample.iterrows():
    employee_id = row['Employee_Id']
    row_filled = row.fillna('')
    full_Name = (
    (str(row['First_Name']) if pd.notna(row['First_Name']) else '') + " " +
    (str(row['Middle_Name']) if pd.notna(row['Middle_Name']) else '') + " " +
    (str(row['Last_Name']) if pd.notna(row['Last_Name']) else '')
).strip()
    condition_hist_table1['Employee_ID'] = employee_id
    condition_hist_table1['Action_type'] = row['Action_Type']
    condition_hist_table1['Action_Reason'] = row['Action_Reason']
    condition_hist_table1['S.No.'] = index+1

################################# Employee details ###################################
    employee_details = employee_details_Master_df[
    (employee_details_Master_df['AxtriaSalesIQST__SIQ_Employee_ID__c'] == employee_id) &
    (employee_details_Master_df['isActive__c'] == True)]    
    employee_details_filled = employee_details.fillna('')
    if (employee_details['AxtriaSalesIQST__SIQ_Country__c'] == 'USA').any():
        print(f"Employee {employee_id}: Country is USA.")
        condition_hist_table1['Test Case'] = 'Checking whether the country is USA or not'
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: Country is USA.")
        condition_hist_table1['Test Case'] = 'Checking whether the country is USA or not'
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The country is NOT USA'
    cond1 = pd.concat([cond1,condition_hist_table1])
        
    if (employee_details['Supervisor_Empl_Id__c'] == row['Supervisor_Empl_Id']).any():
        print(f"Employee {employee_id}: Supervisor Employee id in delta file and Employee file are matching.")
        condition_hist_table1['Test Case'] = 'Checking whether Supervisor Employee id in delta file and Employee file are matching or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else :
        print(f"Employee {employee_id}: Supervisor Employee id in delta file and Employee file are not matching.")
        condition_hist_table1['Test Case'] = 'Checking whether Supervisor Employee id in delta file and Employee file are matching or not.'
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'Supervisor Employee id in delta file and Employee file are not matching.'
    cond1 = pd.concat([cond1,condition_hist_table1])
    if (employee_details['AxtriaSalesIQST__SIQ_Employee_Type__c'] == row['Employee_Type']).any():
        
        print(f"Employee {employee_id}: The Employee type are matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The Employee type are matching from both files or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: The Employee type are not matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The Employee type are matching from both files or not '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The Employee type are not matching from both files '
    cond1 = pd.concat([cond1,condition_hist_table1])
    
    if (employee_details['AxtriaSalesIQST__SIQ_Job_Title_Code__c'] == row['Job_Code']).any():

        print(f"Employee {employee_id}: The Job code are matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The job code are matching from both files or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: The Job code are not matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The job code are matching from both files or not '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The job code are not matching from both files '
    cond1 = pd.concat([cond1,condition_hist_table1])
        
    if (employee_details['AxtriaSalesIQST__SIQ_Job_Title__c'] == row['Job_Title']).any():

        print(f"Employee {employee_id}: The Job title are matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The job title are matching from both files or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: The Job title are not matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The job title are matching from both files or not '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The job title are not matching from both files '
    cond1 = pd.concat([cond1,condition_hist_table1])
    
    if (employee_details_filled['Name'] == full_Name).any():

        print(f"Employee {employee_id}: The Name are matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The name are matching from both files or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: The name are not matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether name are matching from both files or not '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The name are not matching from both files '
    cond1 = pd.concat([cond1,condition_hist_table1])
    
    if (employee_details['SIQ_Position_Code_c__c'] == row['Position_Code']).any():

        print(f"Employee {employee_id}: The Position code are matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The position code are matching from both files or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: The position code are not matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether position code are matching from both files or not '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The position code are not matching from both files '
    cond1 = pd.concat([cond1,condition_hist_table1])
        
    if (employee_details['SIQ_Position_Title_c__c'] == row['Position_Title']).any():

        print(f"Employee {employee_id}: The Position title are matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The position title are matching from both files or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: The position title are not matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether position title are matching from both files or not '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The position title are not matching from both files '
    cond1 = pd.concat([cond1,condition_hist_table1])
        
    if (employee_details['SIQ_Organization_c__c'] == row['Organization']).any():

        print(f"Employee {employee_id}: The Organization are matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The Organization are matching from both files or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: The Organization are not matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether Organization are matching from both files or not '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The Organization are not matching from both files '
    cond1 = pd.concat([cond1,condition_hist_table1])
   
    if (employee_details_filled['SIQ_Supervisor_First_Name_c__c'] == row_filled['Supervisor_First_Name']).any() & (employee_details_filled['SIQ_Supervisor_Last_Name_c__c'] == row_filled['Supervisor_Last_Name']).any() & (employee_details_filled['SIQ_Supervisor_Middle_Name_c__c'] == row_filled['Supervisor_Middle_Name']).any() & (employee_details_filled['SIQ_Supervisor_Nickname_c__c'] == row_filled['Supervisor_Nickname']).any():

        print(f"Employee {employee_id}: The Work Zip, Work country, DSI logon, DSI email are matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The Work Zip, Work country, DSI logon, DSI email are matching from both files or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: The Work Zip, Work country, DSI logon, DSI email are not matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether Work Zip, Work country, DSI logon, DSI email are matching from both files or not '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The SWork Zip, Work country, DSI logon, DSI email are not matching from both files '
    cond1 = pd.concat([cond1,condition_hist_table1])
    
    if (employee_details['SIQ_Work_Zip_c__c'] == row['Work_Zip']).any() & (employee_details['SIQ_Work_Country_c__c'] == row['Work_Country']).any() & (employee_details['SIQ_DSI_Logon_c__c'] == row['DSI_Logon']).any() & (employee_details['SIQ_DSI_Email_Id_c__c'] == row['DSI_Email_Id']).any():

        print(f"Employee {employee_id}: The Supervisor Name are matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The Supervisor Name are matching from both files or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: The Supervisor Name are not matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether Supervisor Name are matching from both files or not '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The Supervisor Name are not matching from both files '
    cond1 = pd.concat([cond1,condition_hist_table1])
        
    if (employee_details['SIQ_Payroll_Id_c__c'] == row['Payroll_Id']).any():

        print(f"Employee {employee_id}: The Payroll ID are matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The Payroll ID are matching from both files or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: The Payroll ID are not matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether Payroll ID are matching from both files or not '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The Payroll ID are not matching from both files '
    cond1 = pd.concat([cond1,condition_hist_table1])
    
    if (employee_details['SIQ_Band_c__c'] == row['Band']).any():

        print(f"Employee {employee_id}: The Band are matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The Band are matching from both files or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: The Band are not matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether Band are matching from both files or not '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The Band are not matching from both files '
    cond1 = pd.concat([cond1,condition_hist_table1])
    
    if (employee_details_filled['SIQ_UserPrefix_c__c'] == row_filled['UserPrefix']).any():

        print(f"Employee {employee_id}: The user Prefix are matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The user Prefix are matching from both files or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: The user Prefix are not matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether user Prefix are matching from both files or not '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The user Prefix are not matching from both files '
    cond1 = pd.concat([cond1,condition_hist_table1])
    
    if (employee_details_filled['SIQ_Leave_Type_c__c'] == row_filled['Leave_Type']).any():

        print(f"Employee {employee_id}: The leave type are matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The leave type are matching from both files or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: The leave type are not matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether leave type are matching from both files or not '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The leave type are not matching from both files '
    cond1 = pd.concat([cond1,condition_hist_table1])
        
    if (employee_details['Personnel_Number__c'] == row['Employee_Id']).any():

        print(f"Employee {employee_id}: The Personeel number are matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The Personeel number are matching from both files or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: The Personeel number are not matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether Personeel number are matching from both files or not '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The Personeel number are not matching from both files '
    cond1 = pd.concat([cond1,condition_hist_table1])
        
    if (employee_details['Employment_Status__c'] == row['Employee_Status']).any():

        print(f"Employee {employee_id}: The Employee status are matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The Personeel number are matching from both files or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: The Employee status are not matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether Employee status are matching from both files or not '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The Employee status are not matching from both files '
    cond1 = pd.concat([cond1,condition_hist_table1])
    
    if (employee_details['Employment_Status__c'] == row['Employee_Status']).any():

        print(f"Employee {employee_id}: The Employee status are matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether The Personeel number are matching from both files or not '
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: The Employee status are not matching from both files")
        condition_hist_table1['Test Case'] = 'Checking whether Employee status are matching from both files or not '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = 'The Employee status are not matching from both files '
    cond1 = pd.concat([cond1,condition_hist_table1])
        
    if (employee_details_filled['AxtriaSalesIQST__SIQ_Gender__c'] == '').all() and (employee_details_filled['AxtriaSalesIQST__SIQ_Field_Status__c'] == '').all():

        
        print(f"Employee {employee_id}: Field status and Gender is Blank.")
        condition_hist_table1['Test Case'] = 'Checking whether Field status and Gender is Blank or not.'
        condition_hist_table1['Status'] = 'Pass'
        condition_hist_table1['Reason'] = ''
    else:
        print(f"Employee {employee_id}: Field status and Gender is not Blank.")
        condition_hist_table1['Test Case'] = 'Checking whether Field status and Gender is Blank or not '
        condition_hist_table1['Status'] = 'Fail'
        condition_hist_table1['Reason'] = ' Field status is not Blank '
    cond1 = pd.concat([cond1,condition_hist_table1])

selected_columns_unique

df_DTA_Records_Sample_Transfer = selected_columns_unique[(selected_columns_unique['type'] == 'Transfer Position code change') | (selected_columns_unique['type'] == 'Transfer Territory code change')]


df_DTA_Records_Sample_Transfer

df_DTA_Records_Sample_Promotion = selected_columns_unique[(selected_columns_unique['type'] == 'Promotion without transfer') | (selected_columns_unique['type'] == 'Promotion with Transfer by Position code')| (selected_columns_unique['type'] =='Promotion with Transfer by Territory code')]

df_DTA_Records_Sample_Promotion



###############               History Table ########################################

import pandas as pd
 
# Load the necessary data
prev_file = employee_history_df_Prev
current_file = employee_history_df
delta_file = df_DTA_Records_Sample_Promotion
 
# Convert Employee_ID__c columns to the same data type (e.g., string)
prev_file['Employee_ID__c'] = prev_file['Employee_ID__c'].astype(str)
current_file['Employee_ID__c'] = current_file['Employee_ID__c'].astype(str)
delta_file['Employee_Id'] = delta_file['Employee_Id'].astype(str)
 
# Filter employees from both files using the delta file
delta_employees = delta_file['Employee_Id'].unique().tolist()
prev_filtered = prev_file[prev_file['Employee_ID__c'].isin(delta_employees)]
current_filtered = current_file[current_file['Employee_ID__c'].isin(delta_employees)]
print(type(delta_employees))
print(type(prev_file))
print(type(current_file))

# Filter out the latest history flag as true in the previous file
prev_latest = prev_filtered[prev_filtered['LatestHistory__c'] == True]
 
# Merge the previous and current files on employee ID
merged_df = pd.merge(current_filtered, prev_latest[['Employee_ID__c', 'Sales_Force_Code__c','IC_Status__c']], on='Employee_ID__c', how='left')
print(prev_file['Employee_ID__c'])
print(merged_df['LatestHistory__c'])

# Rename the Salesforce column from the BU in the previous file to Prev BU column in the current file
merged_df = merged_df.rename(columns={'Sales_Force_Code__c': 'Prev_BU'})
 
# Save the resulting dataframe to a CSV file
merged_df.to_csv('merged_file_with_prev_BU.csv', index=False)

print("The merging process is complete. The output has been saved to 'merged_file_with_prev_BU.csv'.")

# Filter the DataFrame to include only rows where latest_history_flag is True
filtered_df = merged_df[merged_df['LatestHistory__c'] == True]
 
# Sort the DataFrame by Start_Date__c in descending order
sorted_df = filtered_df.sort_values(by='Start_Date__c', ascending=False)

# Pick the first row of the sorted DataFrame
first_row = sorted_df.iloc[0]

# Display the first row (for verification purposes)
sales_force_code_x = first_row['Sales_Force_Code__c_x']
sales_force_code_y = first_row['Sales_Force_Code__c_y']
ic_status_y = first_row['IC_Status__c_y']

# Initialize new_ic_status_x with the original value of IC_Status__c_x
new_ic_status_x = first_row['IC_Status__c_x']
print("Current BU:", sales_force_code_x)
print("Previous BU :",sales_force_code_y)

if sales_force_code_y in ['OncSC','OncSCII']:
    if sales_force_code_x in ['OncSC','OncSCII']:
        if new_ic_status_x == ic_status_y:
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x == 'OncB':
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x == 'OncL':
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x == 'OncHSC':
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x in ['Market Access', 'RML','FMA']:
        if new_ic_status_x == 'CE':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
            
elif sales_force_code_y == 'OncB':
    if sales_force_code_x in ['OncSC','OncSCII']:
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x == 'OncB':
        if new_ic_status_x == ic_status_y:
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x == 'OncL':
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x == 'OncHSC':
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x in ['Market Access', 'RML','FMAB']:
        if new_ic_status_x == 'CE':
            print("IC Status is right")
        else :
            print("IC Status is Not right")

elif sales_force_code_y == 'OncL':
    if sales_force_code_x in ['OncSC','OncSCII']:
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x == 'OncB':
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x == 'OncL':
        if new_ic_status_x == ic_status_y:
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x == 'OncHSC':
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x in ['Market Access', 'RML','FMAB']:
        if new_ic_status_x == 'CE':
            print("IC Status is right")
        else :
            print("IC Status is Not right")

elif sales_force_code_y in ['Market Access','RML','FMAB']:
    if sales_force_code_x in ['OncSC','OncSCII']:
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x == 'OncB':
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x == 'OncL':
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x == 'OncHSC':
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x in ['Market Access', 'RML','FMAB']:
        if new_ic_status_x == ic_status_y:
            print("IC Status is right")
        else :
            print("IC Status is Not right")
            
elif sales_force_code_y == 'OncHSC':
    if sales_force_code_x in ['OncSC','OncSCII']:
        if new_ic_status_x == ic_status_y:
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    
    if sales_force_code_x == 'OncB':
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x == 'OncL':
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x == 'OncHSC':
        if new_ic_status_x == ic_status_y:
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x in ['Market Access', 'RML','FMAB']:
        if new_ic_status_x == ic_status_y:
            print("IC Status is right")
        else :
            print("IC Status is Not right")
            
elif sales_force_code_y == 'HO':
    if sales_force_code_x in ['OncSC','OncSCII']:
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x == 'OncB':
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x == 'OncL':
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x == 'OncHSC':
        if new_ic_status_x == 'TRN':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
    if sales_force_code_x in ['Market Access', 'RML','FMAB']:
        if new_ic_status_x == 'CE':
            print("IC Status is right")
        else :
            print("IC Status is Not right")
        


    
        





import pandas as pd

def check_employee_history(df, employee_history_df, delta_df, employee_history_df_Prev):
    cond1 = pd.DataFrame(columns=['Test Case', 'Status', 'Reason'])
    employee_history_df['End_Date__c'] = employee_history_df['End_Date__c'].replace('3999-12-31', '2262-03-11')

    for index, row in df.iterrows():
        employee_id = row['Employee_Id']
        Product_Name = row['Product']
        # Checking whether it is sales or non sales
        if row['flag_BU_classification'] == 'Non-sales':
            # Check conditions in the Employee History table
            if employee_id in employee_history_df['Employee_ID__c'].values:
                employee_history_row_1 = employee_history_df[employee_history_df['Employee_ID__c'] == employee_id]
                new_line_items = employee_history_df[employee_history_df['Employee_ID__c'] == employee_id]
                filtered_df_current = employee_history_df[employee_history_df['Employee_ID__c'].isin(delta_df['Employee_Id'])]
                filtered_df_previous = employee_history_df_Prev[employee_history_df_Prev['Employee_ID__c'].isin(delta_df['Employee_Id'])]
                new_ids = filtered_df_current[~filtered_df_current['Id'].isin(filtered_df_previous['Id'].values)]
                
                if not new_ids.empty:
                    print(f"Employee {employee_id}: New line is created")
                else:
                    
                    print(f"Employee {employee_id}: New line is not created")

                if not employee_history_row_1.empty:
                    sort_records = employee_history_row_1.sort_values(by='End_Date__c', ascending=False)
                    employee_history_row = sort_records.iloc[1]
                    employee_history_newline = sort_records.iloc[0]

                    
                    if pd.to_datetime(employee_history_row['End_Date__c']) < pd.to_datetime(row['Action_Start_Date']):
                        print(f"Employee {employee_id} and product_Name {Product_Name}: Previous event end-dated.")
                        test_case = 'Checking if the previous event is end-dated or not'
                        status = 'Pass'
                        reason = ''
                    else:
                        test_case = 'Checking if the previous event is end-dated or not'
                        status = 'Fail'
                        reason = 'Previous Event is not end-dated'
                    cond1 = pd.concat([cond1, pd.DataFrame({'Test Case': test_case, 'Status': status, 'Reason': reason}, index=[0])])


                    if not employee_history_newline.empty:
                        new_line_items = pd.DataFrame(employee_history_newline).T
                        employee_id_mask = new_line_items['Employee_ID__c'] == employee_id
                        new_line_items_loa = new_line_items[new_line_items['POSITION_CODE__c'] == row['Position_Code']]


                        if not new_line_items_loa.empty:
                            print(f"Employee {employee_id}: New line items with IC status CE and Position code is same")
                            test_case = 'New line items with IC status CE and Position code is same'
                            status = 'Pass'
                            reason = ''
                        else:
                            print("New line item is not created")
                            test_case = 'checking New line items with IC status CE and Position code is same'
                            status = 'Fail'
                            reason = 'New line items with IC status CE and Position code is not same'
                    cond1 = pd.concat([cond1, pd.DataFrame({'Test Case': test_case, 'Status': status, 'Reason': reason}, index=[1])])
                        
                            
        
        else:
             if employee_id in employee_IC_history_df['Employee_ID__c'].values:
                    employee_history_row_1 = employee_IC_history_df[ (employee_IC_history_df['Employee_ID__c'] == employee_id)]
                    #Sorting with respect to product and end date, because we it's sales component
                    
                    if not employee_history_row_1.empty:
                        employee_history_row_1 = employee_history_row_1.assign(Rank=employee_history_row_1.groupby('Product__c')['End_Date__c'].rank(ascending=False, method='dense'))
                        new_line_items = employee_history_df[employee_history_df['Employee_ID__c'] == employee_id]
                        filtered_df_current = employee_history_df[employee_history_df['Employee_ID__c'].isin(delta_df['Employee_Id'])]
                        filtered_df_previous = employee_history_df_Prev[employee_history_df_Prev['Employee_ID__c'].isin(delta_df['Employee_Id'])]
                        new_ids = filtered_df_current[~filtered_df_current['Id'].isin(filtered_df_previous['Id'].values)]
                        
                        if not new_ids.empty:
                            print(f"Employee {employee_id}: New line is created")
                        else :
                            print(f"Employee {employee_id}: New line is not created")
                        sort_records =  employee_history_row_1.sort_values(by=['Rank', 'Product__c'], ascending=[True, False])
                        employee_history_row =  sort_records[sort_records['Rank'] == 2].iloc[0]
                        employee_history_newline = sort_records[sort_records['Rank']==1]
                        condition_hist_table1['Employee_ID'] = employee_id
                        condition_hist_table1['Action_type'] = row['Action_Type']
                        condition_hist_table1['Action_Reason'] = row['Action_Reason']
                        condition_hist_table1['S.No.'] = index+1
                        condition_hist_table1['Test Case'] = 'Checking if the previous event is end-dated or not'
                         ## Checking whether previous end dated
    
                        if pd.to_datetime(employee_history_row['End_Date__c']) < pd.to_datetime(row['Action_Start_Date']):
                            print(f"Employee {employee_id}: Previous event end-dated.")
                            test_case = 'Checking if the previous event is end-dated or not'
                            status = 'Pass'
                            reason  = ''
                        else:
                            test_case = 'Checking if the previous event is end-dated or not'
                            status = 'Fail'
                            reason  = 'Previous Event is not end-dated'
                        cond1 = pd.concat([cond1, pd.DataFrame({'Test Case': test_case, 'Status': status, 'Reason': reason}, index=[0])])
    #                     print(condition_hist_table1)
    #                     condition1_history_table.concat()
                            #employee_history_1st.append(employee_id)
                         ## checking whether new line is created  
    #                     new_line_items = pd.DataFrame(employee_history_newline).T
    #                    employee_id_mask = new_line_items['Employee_ID__c'] == employee_id
                        

                        new_line_items = employee_history_newline[employee_history_newline['Employee_ID__c'] == employee_id]

                        new_line_items_loa = new_line_items[((new_line_items['POSITION_CODE__c']) == (row['Position_Code'])) & (new_line_items['Product__c'] == row['Product'])]
            
                        if not new_line_items_loa.empty:
                            print(f"Employee {employee_id} and product Name {Product_Name}: New line items with IC status LOA and start date with LOA start date.")
                            #employee_history_2nd.append(employee_id)
                            test_case = 'Checking if the New line items with IC status LOA and start date with LOA start date'
                            status = 'Pass'
                            reason = ''
                        else :
                            print("New line items is not created")
                            test_case = 'Checking if the New line items with IC status LOA and start date with LOA start date'
                            status = 'Fail'
                            reason = 'New line items is not created'
                        cond1 = pd.concat([cond1, pd.DataFrame({'Test Case': test_case, 'Status': status, 'Reason': reason}, index=[1])])
    return cond1
result_check_Employee_history_Transfer=check_employee_history(df_DTA_Records_Sample_Transfer, employee_history_df, delta_df, employee_history_df_Prev)
result_check_Employee_history_Promotion=check_employee_history(df_DTA_Records_Sample_Promotion, employee_history_df, delta_df, employee_history_df_Prev)




if not result_check_Employee_history_Promotion. empty:
    cond2= result_check_Employee_history_Promotion
elif not result_check_Employee_history_Transfer.empty:
    cond2= result_check_Employee_history_Transfer


cond2

### Employee Feed ####################

def check_promotion_events(df, employee_feed_df,event_name):
    cond1 = pd.DataFrame(columns=['Test Case', 'Status', 'Reason'])
    for index, row in df.iterrows():
        employee_feed_row = None
        employee_id = row['Employee_Id']
        
        if employee_id in employee_feed_df['Employee_ID__c'].values:
            employee_feed_row = employee_feed_df[(employee_feed_df['Employee_ID__c'] == employee_id) & 
                                                  (employee_feed_df['AxtriaSalesIQST__Event_Name__c'].str.contains(event_name))]
            if not employee_feed_row.empty and (employee_feed_row['AxtriaSalesIQST__Event_Name__c'] == event_name).any():
                print(f"Employee {employee_id}: Event_Name is 'Promotion' or 'Transfer'.")
                test_case = 'Checking if the Event_Name is Promotion or Transfer'
                status = 'Pass'
                reason = ''
                
            else:
                print(f"Employee {employee_id}: Event_Name is not 'Promotion' or 'Transfer'.")
                test_case = 'Checking if the Event_Name is Promotion or Transfer'
                status = 'Fail'
                reason = 'Event name is not promotion or Transfer'
        else:
            print(f"Employee {employee_id} not found in employee_feed_df.")
        cond1 = pd.concat([cond1, pd.DataFrame({'Test Case': test_case, 'Status': status, 'Reason': reason}, index=[1])])
    return cond1
    

# Usage example:
result_transfer=check_promotion_events(df_DTA_Records_Sample_Transfer, employee_feed_df,'Transfer')
result_promotion=check_promotion_events(df_DTA_Records_Sample_Promotion, employee_feed_df,'Promotion')




if not result_promotion.empty:
    cond3= result_promotion
elif not  result_transfer.empty:
    cond3= result_transfer

cond3

def check_employee_details(df, employee_details_df, condition_hist_table1,Action_type,Action_reason):
    cond1 = pd.DataFrame(columns=['Test Case', 'Status', 'Reason'])
    
    for index, row in df.iterrows():
        emp_id = row['Employee_Id']
        employee_details_records = employee_details_df[employee_details_df['AxtriaSalesIQTM__Employee_ID__c'] ==str(emp_id)]

        if any(status.strip().upper() in ['CE', 'TRN'] for status in employee_details_records['IC_Status__c']):
            
            print(f"Employee {emp_id}: SIC status changed to CE.")
            test_case = 'Checking whether SIC status changed to CE'
            status = 'Pass'
            reason = ''
        else:
            print(f"Employee {emp_id}: SIC status not changed to CE.")
            test_case = 'Checking whether SIC status changed to CE'
            status = 'Fail'
            reason = 'SIC status is not correct'
        cond1 = pd.concat([cond1, pd.DataFrame({'Test Case': test_case, 'Status': status, 'Reason': reason}, index=[0])])

        if employee_details_records[(employee_details_records['Action_Type__c'] == Action_type) & 
                                    (employee_details_records['Action_Reason__c'] == Action_reason)].any().any():
            print(f"Employee {emp_id}: Action Type changed to DTA and Action Reason is Promotion.")
            test_case = 'Checking whether Action Type changed to DTA and Action Reason is Promotion or transfer'
            status = 'Pass'
            reason = ''
        else :
            print(f"Employee {emp_id}: Action Type not changed to DTA and Action Reason is Promotion.")
            test_case = 'Checking whether Action Type changed to DTA and Action Reason is Promotion or transfer'
            status = 'Fail'
            reason = 'Action Type not changed to DTA and Action Reason is not changed to Promotion or Transfer.'
        cond1 = pd.concat([cond1, pd.DataFrame({'Test Case': test_case, 'Status': status, 'Reason': reason}, index=[0])])
    
    return cond1

# Usage example:
result_Employee_details_Transfer = check_employee_details(df_DTA_Records_Sample_Transfer, employee_details_df, condition_hist_table1,'DTA','Promotion')
result_Employee_details_promotion = check_employee_details(df_DTA_Records_Sample_Promotion, employee_details_df, condition_hist_table1,'DTA','Promotion')




if not result_Employee_details_Transfer.empty:
    cond4= result_Employee_details_Transfer
elif not result_Employee_details_promotion.empty:
    cond4= result_Employee_details_promotion

cond4

def check_new_records(df, employee_position, delta_df, employee_position_Prev):
    cond1 = pd.DataFrame(columns=['Test Case', 'Status', 'Reason'])


    for index, row in df.iterrows():
        emp_id = row['Employee_Id']
        filtered_df_current = employee_position[employee_position['AxtriaSalesIQST__Employee_ID__c'].isin(delta_df['Employee_Id'])]
        filtered_df_previous = employee_position_Prev[employee_position_Prev['AxtriaSalesIQST__Employee_ID__c'].isin(delta_df['Employee_Id'])]
        new_ids = filtered_df_current[~filtered_df_current['Id'].isin(filtered_df_previous['Id'].values)]

        if not new_ids.empty:
            print(f"Employee {emp_id}: New line is created")
            test_case = 'Checking whether New record is added or not'
            status = 'Pass'
            reason = ''
            
        else:
            print(f"Employee {emp_id}: New line is not created")
            test_case = 'Checking whether New record is added or not'
            status = 'Fail'
            reason = 'New record is not added'
        cond1 = pd.concat([cond1, pd.DataFrame({'Test Case': test_case, 'Status': status, 'Reason': reason}, index=[0])])
    return cond1
        

# Usage example:
Result_new_record_promotion= check_new_records(df_DTA_Records_Sample_Promotion, employee_position, delta_df, employee_position_Prev)
Result_new_record_transfer= check_new_records(df_DTA_Records_Sample_Transfer, employee_position, delta_df, employee_position_Prev)



if not Result_new_record_promotion.empty:
    cond5= Result_new_record_promotion
elif not Result_new_record_transfer.empty:
    cond5= Result_new_record_transfer

cond5

## Transfer cases Demographic

import pandas as pd

# Function to update condition history table
def update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, test_case, status, reason):
    condition_hist_table1['Employee_ID'] = emp_id
    condition_hist_table1['S.No.'] = index + 1
    condition_hist_table1['Test Case'] = test_case
    condition_hist_table1['Status'] = status
    condition_hist_table1['Reason'] = reason
    return pd.concat([cond1, condition_hist_table1])



# Main processing function
def process_employee_records(df_DTA_Records_Sample_Promotion, employee_details_Master_df, cond1, condition_hist_table1):
    #employee_details_Master_df['AxtriaSalesIQST__SIQ_Employee_ID__c'] = employee_details_Master_df['AxtriaSalesIQST__SIQ_Employee_ID__c'].astype(str)   
    
    for index, row in df_DTA_Records_Sample_Promotion.iterrows():
        emp_id = row['Employee_Id']
        #emp_id = emp_id.astype(str)
        employee_details_records = employee_details_Master_df[
            (employee_details_Master_df['AxtriaSalesIQST__SIQ_Employee_ID__c'] == emp_id) &
            (employee_details_Master_df['isActive__c'] == True)
        ]
        print(employee_details_records)
        if not employee_details_records.empty:
            employee_details_record = employee_details_records.iloc[0]
            employee_details_filled = employee_details_record.fillna('')
            
            # Test 1: Position Start Date vs Assignment Start Date
            if (pd.to_datetime(employee_details_records['SIQ_Assignment_Start_Date_c__c']) == pd.to_datetime(row['Position_Start_Date'])).any() or (pd.to_datetime(employee_details_records['SIQ_Position_Start_Date_c__c']) == pd.to_datetime(row['Action_Start_Date'])).any():
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking whether The position Start date was same as the Assignment Start date or not', 'Pass', ' ')
            else:
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking whether The position Start date was same as the Assignment Start date or not', 'Fail', 'The position Start date was not same as the Assignment Start date')
            
            # Test 2: Assignment End Date
            if employee_details_filled['SIQ_Assignment_End_Date_c__c'] == '':
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking whether Assignment End date is empty or not', 'Pass', ' ')
            else:
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking whether Assignment End date is empty or not', 'Fail', 'Assignment End date was not empty')
            
            # Test 3: Action Start Date
            if (pd.to_datetime(employee_details_records['SIQ_Action_Start_Date_c__c']) == pd.to_datetime(row['Action_Start_Date'])).any():
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking whether Action start date are same or not', 'Pass', '')
            else:
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking whether Action start date are same or not', 'Fail', 'Action start date was not correct')
            
            # Test 4: Action End Date
            if employee_details_filled['SIQ_Action_End_Date_c__c'] == '':
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking whether Action end date is empty or not', 'Pass', '')
            else:
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking whether Action end date is empty or not', 'Fail', 'Action End date was not empty')
            
            # Test 5: Position Start Date vs Action Start Date or Position Start Date
            if (pd.to_datetime(employee_details_records['SIQ_Position_Start_Date_c__c']) == pd.to_datetime(row['Action_Start_Date'])).any() or (pd.to_datetime(employee_details_records['SIQ_Position_Start_Date_c__c']) == pd.to_datetime(row['Position_Start_Date'])).any():
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking whether Position start date is match with either Action start date or position start date or not', 'Pass', '')
            else:
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking whether Position start date is match with either Action start date or position start date or not', 'Fail', 'Position start date is match with neither Action start date nor position start date')
            
            # Test 6: Position End Date
            if employee_details_filled['SIQ_Position_End_Date_c__c'] == '':
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking position End date is empty or not', 'Pass', '')
            else:
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking position End date is empty or not', 'Fail', 'position end date is not empty')
            
            # Test 7: Last Hire Date
            if (pd.to_datetime(employee_details_records['Last_Hire_Date__c']) == pd.to_datetime(row['Last_Hire_Date'])).any():
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking Last Hire date should match with the Delta Last Hire date or not', 'Pass', '')
            else:
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking Last Hire date should match with the Delta Last Hire date or not', 'Fail', 'Last Hire date not matching with the Delta Last Hire date')
            
            # Test 8: Original Hire Date
            if (pd.to_datetime(employee_details_records['Original_Hire_Date__c']) == pd.to_datetime(row['Original_Hire_Date'])).any():
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking original hire date should be match with the delta original hire date', 'Pass', '')
            else:
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking original hire date should be match with the delta original hire date', 'Fail', 'original hire date not matching with the delta original hire date')
            
            # Test 9: FTE Start Date
            if (pd.to_datetime(employee_details_records['FTE_Start_Date__c']) == pd.to_datetime(row['FTE_Start_Date'])).any():
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking whether FTE start date should be match with the delta FTE start date or not', 'Pass', '')
            else:
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking whether FTE start date should be match with the delta FTE start date or not', 'Fail', 'FTE start date not matching with the delta FTE start date')
            
            # Test 10: Demotion Date
            if (pd.to_datetime(employee_details_records['SIQ_Demotion_date_c__c']) == pd.to_datetime(row['Demotion_date'])).any():
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking whether Demotion date should be match with the delta demotion date or not', 'Pass', '')
            else:
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking whether Demotion date should be match with the delta demotion date or not', 'Fail', 'Demotion date not matching with the delta demotion date')
            
            # Test 11: AxtriaSalesIQST Original Hire Date
            if (pd.to_datetime(employee_details_records['AxtriaSalesIQST__SIQ_Hire_Date__c']) == pd.to_datetime(row['Original_Hire_Date'])).any():
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking whether Original hire date should be match with the delta original hire date or not', 'Pass', '')
            else:
                cond1 = update_condition_hist_table(condition_hist_table1, cond1, index, emp_id, 'Checking whether Original hire date should be match with the delta original hire date or not', 'Fail', 'Original hire date not match with the delta original hire date')

    return cond1


Result_demographic_transfer=process_employee_records(df_DTA_Records_Sample_Transfer, employee_details_Master_df, cond1, condition_hist_table1)
Result_demographic_promotion=process_employee_records(df_DTA_Records_Sample_Promotion, employee_details_Master_df, cond1, condition_hist_table1)


Result_demographic_promotion


if not Result_demographic_transfer.empty:
    cond6= Result_demographic_transfer
elif not Result_demographic_promotion.empty:
    cond6= Result_demographic_promotion

Final_Output = pd.concat([cond2, cond3,cond4,cond5,cond6])
Final_Output.drop_duplicates(inplace=True)
Final_Output

Final_Output
file_path = 'Final Output/Roster_Output.xlsx'
Final_Output.to_excel(file_path, index=False)


