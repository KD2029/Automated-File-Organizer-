import os
import shutil
import pandas as pd
import logging
from fuzzywuzzy import fuzz
from pathlib import Path
import json
import hashlib

# Set up logging
logging.basicConfig(
    filename='file_organizer.log',  # Specify the log file name
    filemode='a',                    # Append mode
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO                # Set the logging level to INFO
)

# Log the start of the script
logging.info("Script started.")

# File paths
file_path = '/home/demus/Documents/File_Structure.ods'
processed_individuals_file = '/home/demus/Downloads/processed_individuals.ods'
hierarchy_cache_file = '/home/demus/Downloads/hierarchy_cache.json'
base_dir = '/home/demus/Scans_2023 - 2024'
dest_base_dir = '/home/demus/Downloads/Organized_Scans'
hierarchy_excel_file = '/home/demus/Downloads/hierarchy_structure.xlsx'

# Initialize processed_individuals DataFrame
if os.path.exists(processed_individuals_file):
    logging.info(f'{processed_individuals_file} exists. Loading data.')
    processed_df = pd.read_excel(processed_individuals_file, engine='odf')
else:
    logging.info(f'{processed_individuals_file} does not exist. Creating a new file.')
    processed_df = pd.DataFrame(columns=['Individual Name', 'Parish Name', 'Status'])
    processed_df.to_excel(processed_individuals_file, index=False, engine='odf')

# Initialize hierarchy cache and not found counts.
hierarchy_cache = {}
not_found_counts = {}

def get_directory_hash(directory):
    """ Generate a hash of the directory structure to detect changes. """
    dir_hash = hashlib.sha256()
    for root, dirs, files in os.walk(directory):
        for d in dirs:
            dir_hash.update(d.encode())
        for f in files:
            dir_hash.update(f.encode())
    return dir_hash.hexdigest()

def load_hierarchy():
    global hierarchy_cache
    current_hash = get_directory_hash(base_dir)

    if os.path.exists(hierarchy_cache_file):
        logging.info('Loading hierarchy from cache.')
        with open(hierarchy_cache_file, 'r') as f:
            cached_data = json.load(f)

        # Check if directory structure has changed
        if cached_data.get("hash") == current_hash:
            logging.info('No changes in the base directory. Using cached hierarchy.')
            hierarchy_cache = cached_data.get("hierarchy")
        else:
            logging.info('Changes detected in the base directory. Reprocessing hierarchy.')
            hierarchy_cache = process_hierarchy()
            save_hierarchy_to_cache(current_hash)  # Save with the new hash
    else:
        logging.info('No cached hierarchy found. Processing and caching hierarchy.')
        hierarchy_cache = process_hierarchy()
        save_hierarchy_to_cache(current_hash)

def save_hierarchy_to_cache(current_hash):
    """ Save the hierarchy and the current hash to cache. """
    with open(hierarchy_cache_file, 'w') as f:
        cache_data = {"hierarchy": hierarchy_cache, "hash": current_hash}
        json.dump(cache_data, f)
    save_hierarchy_to_excel()

def save_hierarchy_to_excel():
    """ Save the hierarchy to an Excel file. """
    if not hierarchy_cache or 'Archdeaconry' not in hierarchy_cache:
        logging.error("Hierarchy is empty or malformed. Cannot save to Excel.")
        return

    archdeaconry_list = []
    parish_list = []
    sub_parish_list = []
    # Go through the hierarchy and collect data
    for arch, parishes in hierarchy_cache['Archdeaconry'].items():
        for parish, sub_parishes in parishes.items():
            if sub_parishes:
                for sub_parish in sub_parishes:
                    archdeaconry_list.append(arch)
                    parish_list.append(parish)
                    sub_parish_list.append(sub_parish)
            else:
                # If there are no sub-parishes, append an empty string
                archdeaconry_list.append(arch)
                parish_list.append(parish)
                sub_parish_list.append("")

    # Log the number of entries being saved
    logging.info(f"Saving {len(archdeaconry_list)} entries to {hierarchy_excel_file}")

    # Create DataFrame from the lists
    hierarchy_df = pd.DataFrame({
        'Archdeaconry': archdeaconry_list,
        'Parish': parish_list,
        'Sub-Parish': sub_parish_list
    })

    # Save the DataFrame to Excel
    try:
        hierarchy_df.to_excel(hierarchy_excel_file, index=False)
        logging.info(f"Hierarchy successfully saved to {hierarchy_excel_file}")
    except Exception as e:
        logging.error(f"Failed to save hierarchy to Excel: {e}")

def process_hierarchy():
    hierarchy = {'Archdeaconry': {}, 'Parish': {}, 'Sub-Parish': {}}
    for root, dirs, files in os.walk(base_dir):
        for d in dirs:
            if 'arch' in d.lower():
                hierarchy['Archdeaconry'][d] = {}
            elif 'parish' in d.lower():
                parent_arch = os.path.basename(root)
                if parent_arch in hierarchy['Archdeaconry']:
                    hierarchy['Archdeaconry'][parent_arch][d] = {}
            elif 'sub parish' in d.lower():
                parent_parish = os.path.basename(root)
                for arch in hierarchy['Archdeaconry']:
                    if parent_parish in hierarchy['Archdeaconry'][arch]:
                        hierarchy['Archdeaconry'][arch][parent_parish][d] = {}
    return hierarchy

def find_individual_folder(base_dir, individual_name):
    """ Recursively search for an individual folder with a high similarity to the individual name. """
    for root, dirs, files in os.walk(base_dir):
        for dir_name in dirs:
            # Lowercase conversion for case-insensitive matching
            lower_individual_name = individual_name.lower()
            lower_dir_name = dir_name.lower()
            
            # Partial matching with a threshold of 80% similarity
            similarity = fuzz.token_set_ratio(lower_individual_name, lower_dir_name)
            if similarity > 80:
                return os.path.join(root, dir_name), dir_name
    return None, None

def process_individual(individual, parish_name):
    global processed_df
    # Skip already processed individuals
    processed_status = processed_df[processed_df['Individual Name'].str.strip() == individual]['Status'].values
    if processed_status.size > 0:
        status = processed_status[0]
        if status == 'File Exists':
            logging.info(f'Individual {individual} already processed. Skipping.')
            return
        elif status == 'Folder Not Found':
            logging.info(f'Reprocessing individual {individual}.')

    # Find the individual folder
    individual_folder_path, actual_folder_name = find_individual_folder(base_dir, individual)
    if individual_folder_path:
        logging.info(f'Found individual folder for {individual}: {individual_folder_path}')
        
        # Add the additional characters from the folder name to the individual name
        individual_with_folder_name = f"{individual} ({actual_folder_name})"
        logging.info(f"Updated individual name with folder name: {individual_with_folder_name}")
        
        # Find the correct location in the hierarchy
        found_parish = None
        found_archdeaconry = None

        # Search for the parish directly in the cached hierarchy
        for arch, parishes in hierarchy_cache['Archdeaconry'].items():
            if parish_name in parishes:
                found_parish = parish_name
                found_archdeaconry = arch
                break
        
        if found_parish:
            # Log the Archdeaconry and Parish where the match was found
            logging.info(f"Match found: Archdeaconry '{found_archdeaconry}', Parish '{found_parish}' for individual '{individual}'.")

            sub_parish = None
            sub_parishes = hierarchy_cache['Archdeaconry'][found_archdeaconry].get(found_parish, {})
            for sp in sub_parishes:
                if fuzz.ratio(parish_name.lower(), sp.lower()) > 90:
                    sub_parish = sp
                    break

            # Determine the destination path
            dest_path = os.path.join(dest_base_dir, found_archdeaconry, found_parish, actual_folder_name)

            # Check if the destination path already exists
            if os.path.exists(dest_path):
                logging.info(f"Folder '{dest_path}' already exists for individual '{individual}'. Logging and skipping.")
                processed_df.loc[processed_df['Individual Name'] == individual, 'Status'] = 'File Exists'
                return  # Move on to the next individual

            # Copy the folder to the destination
            try:
                shutil.copytree(individual_folder_path, dest_path)
                logging.info(f"Copied individual folder '{actual_folder_name}' to '{dest_path}'.")
                processed_df.loc[processed_df['Individual Name'] == individual, 'Status'] = 'File Exists'
            except Exception as e:
                logging.warning(f"Failed to copy folder '{actual_folder_name}' for '{individual}': {e}")
                processed_df.loc[processed_df['Individual Name'] == individual, 'Status'] = 'Error'
        else:
            # If parish is not found, log the folder not found message
            not_found_counts[individual] = not_found_counts.get(individual, 0) + 1
            
            if not_found_counts[individual] < 4:
                logging.warning(f"Folder for individual '{individual}' not found in parish '{parish_name}'. Attempt {not_found_counts[individual]}.")
                processed_df.loc[processed_df['Individual Name'] == individual, 'Status'] = 'Folder Not Found'
            else:
                logging.info(f"Folder for individual '{individual}' has been logged as not found 3 times. Skipping further logs.")
                processed_df.loc[processed_df['Individual Name'] == individual, 'Status'] = 'Folder Not Found (Max Attempts Reached)'

def main():
    load_hierarchy()  # Load or process the hierarchy

    # Read individual names and parish names from the file structure Excel
    try:
        individuals_df = pd.read_excel(file_path, engine='odf')
        
        # Convert individual names and parish names to strings, and filter out NaN values
        individuals_df['Individual Name'] = individuals_df['Individual Name'].astype(str).str.strip()
        individuals_df['Parish Name'] = individuals_df['Parish Name'].astype(str).str.strip()

        # Drop rows where 'Individual Name' or 'Parish Name' is empty
        individuals_df.dropna(subset=['Individual Name', 'Parish Name'], inplace=True)

        individual_names = individuals_df['Individual Name'].tolist()
        parish_names = individuals_df['Parish Name'].tolist()
    except Exception as e:
        logging.error(f"Error reading individual and parish names from Excel: {e}")
        return

    # Process each individual with their corresponding parish name
    for individual, parish_name in zip(individual_names, parish_names):
        process_individual(individual, parish_name)

    # Save the updated processed individuals DataFrame
    processed_df.to_excel(processed_individuals_file, index=False, engine='odf')
    logging.info(f"Updated processed individuals saved to {processed_individuals_file}")

if __name__ == "__main__":
    main()
