import pyodbc
import pandas as pd

# Connect to Local Instance (SQL Server Management Studio)
server = '.\RP_Analytics'
database = 'LOCAL'

# Create connection string
conn_str = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        f"Server={server};"
        f"Database={database};"
        r"Trusted_Connection=yes"
    )

# Establish Connection and Cursor
conn = pyodbc.connect(conn_str)


# execute a query (must be a select statement until further investigation)
# results are currently stored in "Results2" table within SQL
query = 'SELECT * FROM Results2 WHERE HighwayClassCode = 19'

# return the results
df1 = pd.read_sql_query(query, conn)
conn.close()

df1 = pd.DataFrame(df1)
df1

# grab only the necessary fields from df
data = df1[['LRSID', 'AADT', 'BeginLogMile', 'EndLogMile', 'HighwayClassCode', 'SegmentLength']]
data

# creating condition to implement Scenario-1
# Scenario-1
# o   LSRID is equal AND
# o   Highway Class is equal AND
# o   Segment is adjacent AND
# o   AADT is (within 5%) OR (Null)
# o   Then combine the two


# Sort the data by LRSID and BeginLogMile
data.sort_values(by=['LRSID', 'HighwayClassCode', 'BeginLogMile'], inplace=True)

import pandas as pd

# Assuming 'data' contains your dataset
# Load your data into a pandas DataFrame
# data = pd.read_csv('your_data.csv')

# Sort the data by LRSID and BeginLogMile
data.sort_values(by=['LRSID', 'HighwayClassCode', 'BeginLogMile'], inplace=True)

# Group segments based on the criteria
grouped_segments = []
current_group = []

for i in range(len(data) - 1):
    current_segment = data.iloc[i]
    next_segment = data.iloc[i + 1]

    if (current_segment['LRSID'] == next_segment['LRSID'] and
            current_segment['HighwayClassCode'] == next_segment['HighwayClassCode'] and
            current_segment['EndLogMile'] == next_segment['BeginLogMile']):

        if (current_segment['AADT'] is None or next_segment['AADT'] is None or
                abs(current_segment['AADT'] - next_segment['AADT']) / current_segment['AADT'] <= 0.05):

            current_group.append(current_segment)
        else:
            current_group.append(current_segment)
            grouped_segments.append(current_group)
            current_group = []

grouped_segments.append(current_group)  # Append the last group

# Process the grouped segments
processed_segments = []
for group in grouped_segments:
    if group:  # Check if the group is not empty
        min_begin_log_mile = min(group, key=lambda x: x['BeginLogMile'])['BeginLogMile']
        max_end_log_mile = max(group, key=lambda x: x['EndLogMile'])['EndLogMile']
        total_segment_length = sum([segment['SegmentLength'] for segment in group])
        avg_aadt = sum([segment['AADT'] for segment in group if segment['AADT'] is not None]) / len(group)

        processed_segments.append({
            'LRSID': group[0]['LRSID'],
            'HighwayClassCode': group[0]['HighwayClassCode'],
            'BeginLogMile': min_begin_log_mile,
            'EndLogMile': max_end_log_mile,
            'TotalSegmentLength': total_segment_length,
            'AvgAADT': avg_aadt
        })

# Convert processed_segments to DataFrame
processed_df = pd.DataFrame(processed_segments)

# Output the processed DataFrame
print(processed_df)

processed_df.to_csv('scratch_results.csv')