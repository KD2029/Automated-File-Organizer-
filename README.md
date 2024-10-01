# Automated-File-Organizer-


A Python script designed to efficiently organize files based on their hierarchical structure (Archdeaconry, Parish, Sub-Parish). This script is particularly useful for organizing large collections of files related to specific geographic regions.

# Installation

Install Requirements:
Bash
pip install -r requirements.txt

Usage

# Configure Settings:

Edit the file_organizer.py script to set the following paths:
file_path: Path to the Excel file containing individual names and parish names.
base_dir: Path to the directory containing the unorganized files.
dest_base_dir: Path to the directory where organized files will be placed.
hierarchy_cache_file: Path to the file for caching the hierarchy.
processed_individuals_file: Path to the file for storing processed individuals.
Customize other settings as needed.
Run the Script:

Bash
python file_organizer.py


# Features

Hierarchy Processing: Automatically processes the directory structure to identify the levels.
Individual Folder Matching: Uses fuzzy matching to find individual folders based on names.
File Placement: Accurately places files within their corresponding location.
Caching: Caches the hierarchy for faster processing on subsequent runs.
Logging: Provides detailed logging for troubleshooting and monitoring.
Customization

Hierarchy Structure: The script assumes a specific hierarchy. You can modify the process_hierarchy function to adapt to different structures.
Matching Algorithm: Customize the fuzzy matching algorithm used for finding individual folders.
File Naming Conventions: Modify the script to handle specific file naming conventions.
