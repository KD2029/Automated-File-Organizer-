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
    filename='file_organizer.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

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

# Initialize hierarchy cache
hierarchy_cache = {}
not_found_counts = {}

def get_directory_hash(directory):
    """Generate a hash of the directory structure to detect changes."""
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

        if cached_data.get("hash") == current_hash:
            logging.info('No changes in the base directory. Using cached hierarchy.')
            hierarchy_cache = cached_data.get("hierarchy")
        else:
            logging.info('Changes detected in the base directory. Reprocessing hierarchy.')
            hierarchy_cache = process_hierarchy()
            save_hierarchy_to_cache(current_hash)
    else:
        logging.info('No cached hierarchy found. Processing and caching hierarchy.')
        hierarchy_cache = process_hierarchy()
        save_hierarchy_to_cache(current_hash)

def save_hierarchy_to_cache(current_hash):
    """Save the hierarchy and the current hash to cache."""
    with open(hierarchy_cache_file, 'w') as f:
        cache_data = {"hierarchy": hierarchy_cache, "hash": current_hash}
        json.dump(cache_data, f)
    save_hierarchy_to_excel()

def save_hierarchy_to_excel():
    """Save the hierarchy to an Excel file."""
    if not hierarchy_cache or 'Archdeaconry' not in hierarchy_cache:
        logging.error("Hierarchy is empty or malformed. Cannot save to Excel.")
        return

    archdeaconry_list = []
    parish_list = []
    sub_parish_list = []

    for arch, parishes in hierarchy_cache['Archdeaconry'].items():
        for parish, sub_parishes in parishes.items():
            archdeaconry_list.append(arch)
            parish_list.append(parish)
            sub_parish_list.extend([f"{parish} - {sub_parish}" for sub_parish in sub_parishes])

    # Ensure all lists have the same length by padding with empty strings
    max_length = max(len(archdeaconry_list), len(parish_list), len(sub_parish_list))
    archdeaconry_list.extend([""] * (max_length - len(archdeaconry_list)))
    parish_list.extend([""] * (max_length - len(parish_list)))
    sub_parish_list.extend([""] * (max_length - len(sub_parish_list)))

    logging.info(f"Saving {len(archdeaconry_list)} entries to {hierarchy_excel_file}")

    hierarchy_df = pd.DataFrame({
        'Archdeaconry': archdeaconry_list,
        'Parish': parish_list,
        'Sub-Parish': sub_parish_list
    })

    try:
        hierarchy_df.to_excel(hierarchy_excel_file, index=False)
        logging.info(f"Hierarchy successfully saved to {hierarchy_excel_file}")
    except Exception as e:
        logging.error(f"Failed to save hierarchy to Excel: {e}")

def process_hierarchy():
    """Process the base directory hierarchy to capture Archdeaconry, Parish, and Sub-Parish correctly."""
    hierarchy = {'Archdeaconry': {}}

    # First, identify all Archdeaconries
    for root, dirs, files in os.walk(base_dir):
        for d in dirs:
            lower_d = d.lower()
            # Check if the current directory is an archdeaconry
            if 'arch' in lower_d:
                archdeaconry = d
                hierarchy['Archdeaconry'][archdeaconry] = {}  # Initialize the archdeaconry
                logging.info(f"Found Archdeaconry: {archdeaconry}")

                # Now check for parishes within this archdeaconry
                parish_path = os.path.join(root, d)
                for parish_dir in os.listdir(parish_path):
                    if 'parish' in parish_dir.lower():
                        hierarchy['Archdeaconry'][archdeaconry][parish_dir] = []  # Initialize with an empty list for sub-parishes
                        logging.info(f"Found Parish: {parish_dir} under Archdeaconry: {archdeaconry}")

                        # Now check for sub-parishes within this parish
                        sub_parish_path = os.path.join(parish_path, parish_dir)
                        for sub_parish_dir in os.listdir(sub_parish_path):
                            if 'sub parish' in sub_parish_dir.lower() or 'sub-parish' in sub_parish_dir.lower():
                                hierarchy['Archdeaconry'][archdeaconry][parish_dir].append(sub_parish_dir)  # Add to the sub-parish list
                                logging.info(f"Found Sub-Parish: {sub_parish_dir} under Parish: {parish_dir} in Archdeaconry: {archdeaconry}")

    return hierarchy





def find_individual_folder(base_dir, individual_name):
    """Recursively search for an individual folder with a high similarity to the individual name."""
    if pd.isna(individual_name):
        logging.warning(f"Individual name is NaN. Skipping search.")
        return None, None

    individual_name = str(individual_name)
    
    for root, dirs, files in os.walk(base_dir):
        for dir_name in dirs:
            lower_individual_name = individual_name.lower()
            lower_dir_name = dir_name.lower()

            similarity = fuzz.token_set_ratio(lower_individual_name, lower_dir_name)
            if similarity > 80:
                return os.path.join(root, dir_name), dir_name
    return None, None

def process_individual(individual, parish_name):
    global processed_df

    processed_status = processed_df[processed_df['Individual Name'].str.strip() == individual]['Status'].values
    
    if processed_status.size == 0:
        logging.info(f"Processing new entry for {individual}.")
    elif processed_status.size > 0:
        status = processed_status[0]
        if status == 'File Exists':
            logging.info(f'Individual {individual} already processed. Skipping.')
            return
        elif status == 'Folder Not Found':
            logging.info(f'Reprocessing individual {individual}.')

    individual_folder_path, actual_folder_name = find_individual_folder(base_dir, individual)
    
    if individual_folder_path:
        logging.info(f'Found individual folder for {individual}: {actual_folder_name}')
        
        individual_with_folder_name = f"{individual} ({actual_folder_name})"
        logging.info(f"Updated individual name with folder name: {individual_with_folder_name}")

        found_parish = None
        found_archdeaconry = None
        found_sub_parish = None

        # Search for the parish or sub-parish in the cached hierarchy
        for arch, parishes in hierarchy_cache['Archdeaconry'].items():
            if parish_name in parishes:
                found_parish = parish_name
                found_archdeaconry = arch
                break
            else:
                for sub_parish in parishes.values():
                    if parish_name in sub_parish:  # Check if parish name is in the sub-parish list
                        found_sub_parish = parish_name
                        found_parish = list(parishes.keys())[list(parishes.values()).index(sub_parish)]  # Get the corresponding parish
                        found_archdeaconry = arch
                        break

        if found_parish:
            if found_sub_parish:
                destination_folder = os.path.join(
                    dest_base_dir, found_archdeaconry, found_parish,
                    found_sub_parish
                )
            else:
                destination_folder = os.path.join(
                    dest_base_dir, found_archdeaconry, found_parish
                )
                
            logging.info(f"Destination folder for {individual}: {destination_folder}")
            Path(destination_folder).mkdir(parents=True, exist_ok=True)
            
            destination_path = os.path.join(destination_folder, individual_with_folder_name)
            if not os.path.exists(destination_path):
                shutil.copytree(individual_folder_path, destination_path)
                processed_df = pd.concat([processed_df, pd.DataFrame([{
                    'Individual Name': individual,
                    'Parish Name': parish_name,
                    'Status': 'File Exists'
                }])], ignore_index=True)
                logging.info(f"Copied {individual} to {destination_path}.")
            else:
                logging.warning(f"Destination already exists: {destination_path}. Skipping copy.")
        else:
            if parish_name not in not_found_counts:
                not_found_counts[parish_name] = 0
            not_found_counts[parish_name] += 1
            logging.warning(f"Parish not found in hierarchy: {parish_name}. Total not found: {not_found_counts[parish_name]}.")

            processed_df = pd.concat([processed_df, pd.DataFrame([{
                'Individual Name': individual,
                'Parish Name': parish_name,
                'Status': 'Folder Not Found'
            }])], ignore_index=True)
    else:
        logging.warning(f"No folder found for individual {individual}.")
        processed_df = pd.concat([processed_df, pd.DataFrame([{
            'Individual Name': individual,
            'Parish Name': parish_name,
            'Status': 'Folder Not Found'
        }])], ignore_index=True)

def main():
    load_hierarchy()

    # Read individual names and parish names from the Excel file
    individuals_df = pd.read_excel(file_path, engine='odf')
    individuals_df['Individual Name'] = individuals_df['Individual Name'].replace('nan', '').fillna('')
    individuals_df['Parish Name'] = individuals_df['Parish Name'].replace('nan', '').fillna('')

    for _, row in individuals_df.iterrows():
        individual = row['Individual Name'].strip()
        parish_name = row['Parish Name'].strip()
        
        if individual and parish_name:
            process_individual(individual, parish_name)

    processed_df.to_excel(processed_individuals_file, index=False, engine='odf')

if __name__ == '__main__':
    main()
