import os
import gzip

# Run this script to extract the compressed tasks

output_folder = 'task_events_extracted'
os.makedirs(output_folder, exist_ok=True)

gzip_folder = './task_events'

# Loop through each gzip file in the folder
for filename in os.listdir(gzip_folder):
    if filename.endswith('.csv.gz'):
        file_path = os.path.join(gzip_folder, filename)
        output_file_path = os.path.join(output_folder, filename[:-3]) 
        
        with gzip.open(file_path, 'rb') as f_in:
            with open(output_file_path, 'wb') as f_out:
                f_out.write(f_in.read())

print("Extraction complete.")