import os
import logging

# Set up logging
logging.basicConfig(filename='file_checker.log', level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Set the base directory
base_dir = '/home/demus/Scans_2023 - 2024/Kazo Arch/Masuulita Parish/St.Andrews Masuulita Parish'

# Check if the directory exists
if os.path.exists(base_dir):
    print(f'The directory {base_dir} exists.')
else:
    print(f'The directory {base_dir} does not exist.')
