import csv

# Function to convert text file to CSV
def convert_to_csv(input_file, output_file):
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # Write headers
        writer.writerow(['arrival_time', 'job_id', 'job_size'])
        
        # Iterate over rows in the input file and write to CSV
        i = 0
        for row in reader:
            if not i % 1000000:
                print(f"Done row {i}")
            writer.writerow(row)
            i += 1

# Input and output file paths
input_file = 'task_lines.txt'
output_file = 'jobs.csv'

# Convert text file to CSV
convert_to_csv(input_file, output_file)

print("Conversion complete!")
