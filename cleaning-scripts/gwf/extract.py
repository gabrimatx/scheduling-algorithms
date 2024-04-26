import sqlite3
import csv

conn = sqlite3.connect('Grid5000.sqlite')
cursor = conn.cursor()

cursor.execute('SELECT SubmitTime, RunTime, ExecutableID FROM Jobs WHERE RunTime > -1 AND Status = 1 ORDER by SubmitTime;')

# Fetch all rows from the result set
rows = cursor.fetchall()

# Specify the path for the CSV file
csv_file_path = 'output.csv'

# Write the query result to a CSV file
with open(csv_file_path, 'w', newline='') as csvfile:
    # Create a CSV writer object
    csv_writer = csv.writer(csvfile)
    
    # Write header if needed
    csv_writer.writerow(['arrival_time', 'job_size', 'job_id'])
    
    # Write rows to the CSV file
    csv_writer.writerows(rows)

# Close the database connection
conn.close()
