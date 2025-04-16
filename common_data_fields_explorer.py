#!/usr/bin/env python
# coding: utf-8

'''
Title: Common Data Fields Explorer

Date: 20250415

Author: telook

Purpose: This script was written for the purposes of finding common data field linkages between multiple sheets within 
a single excel file. The final output is to help identify data linkages to be used in the construction of MongoDB pipelines for
further data analysis.

Note: adjust the script as needed for the excel file to be used, the sheet names relevant to
the excel sheet to be used, and export file info.
'''


#Dependencies
import pandas as pd
import numpy as np
from collections import Counter
from openpyxl import load_workbook
import os


#data files
orig_excel = 'Add your own pathway and file'


#read files/create DFs
xl = pd.ExcelFile(orig_excel)

# get all the sheet names in the excel file
sheet_names_all = xl.sheet_names

print(sheet_names_all)


# create a list of sheet names to be scraped for columns. 
# Replace these sheet names with the names in the excel file
sheet_names_used = ['sheet_0', 
                    'sheet_1', 
                    'sheet_2', 
                    'sheet_3', 
                    'sheet_4']


# For Exploratory purposes
# Create a dictionary with Key: sheet_names and Value: column names present in each respective sheet_name

dict_sheet_name_columns = {}

for sheet_name in sheet_names_used:
    df = pd.read_excel(orig_excel, sheet_name=sheet_name)
    sheet_name = sheet_name
    column_names = df.columns
    dict_sheet_name_columns[sheet_name] = column_names 
    
    print(f'Sheet name: {sheet_name} has the following columns:{column_names}')
    

# Create a padded dictionary. Key: Sheet names , Value: column names of a sheet
# using a padded dictionary approach so that a DF with all of the excel file sheet sheet names for are used for the column names 
# and the column names from each excel sheet will be the row values and then can be easily viewed and manipulated as needed.

# Find the maximum number of columns in any sheet
max_cols = max(len(columns) for columns in dict_sheet_name_columns.values())

# Create a new dictionary with padded lists
df_dict = {}
for sheet_name, columns in dict_sheet_name_columns.items():
    # Pad with NaN to make all lists the same length
    padded_columns = list(columns) + [""] * (max_cols - len(columns))
    df_dict[sheet_name] = padded_columns

# Create the DataFrame
df_sht_clmn = pd.DataFrame(df_dict)

# Display the DataFrame
df_sht_clmn.sample(10)


# Function to search for exact value matches across the columns, list the value identified along with the count, and
# sort the new DF by most number of matches to lowest number of matches

def find_exact_matches_across_columns(df):
    # Create a dictionary to store values and their positions
    value_positions = {}
    
    # Go through each column and collect positions of each value
    for col in df.columns:
        for idx, value in enumerate(df[col]):
            # Skip empty values
            if pd.isna(value) or value == '':
                continue
                
            # Convert to string to handle different types
            value_str = str(value)
            
            # Initialize dict entry if not exists
            if value_str not in value_positions:
                value_positions[value_str] = []
                
            # Add this position (col, idx)
            value_positions[value_str].append((col, idx))
    
    # Filter to keep only values that appear in multiple columns
    common_values = {val: positions for val, positions in value_positions.items() 
                     if len(set(col for col, _ in positions)) > 1}
    
    # Create result data
    result_rows = []
    
    for value, positions in common_values.items():
        # Count unique columns where this value appears
        cols_with_value = set(col for col, _ in positions)
        match_count = len(cols_with_value)
        
        # Create a row where the value appears in matched columns, empty string elsewhere
        row = {col: value if col in cols_with_value else '' for col in df.columns}
        
        # Add match count
        row['Match_Count'] = match_count
        row['Value'] = value
        
        result_rows.append(row)
    
    # Create DataFrame and sort by match count
    if result_rows:
        result_df = pd.DataFrame(result_rows)
        
        # Sort by match count (descending)
        result_df = result_df.sort_values('Match_Count', ascending=False)
        
        # Reorder columns to have Value and Match_Count first
        cols = ['Value', 'Match_Count'] + [col for col in df.columns]
        result_df = result_df[cols]
        
        return result_df
    else:
        # Return empty DataFrame with correct columns if no matches
        return pd.DataFrame(columns=['Value', 'Match_Count'] + list(df.columns))


# Call the find exact matches across columns function
match_df = find_exact_matches_across_columns(df_sht_clmn)

# uncomment to see results
# match_df.head(10)


# This function is to write the new DF data to the original excel file in a new tab/sheet so that it can be used as a reference for
# helping to determine what linkages and sheets to use in the DB pipeline construction

def add_df_to_excel(df, file_path="original_file.xlsx", sheet_name="common_fields_tab"):
    """
    Simplest approach: just create a new file with a different name first
    """
    try:
        # Create a new filename
        new_file = f"{os.path.splitext(file_path)[0]}_updated.xlsx"
        
        # Check if the original file exists
        if os.path.exists(file_path):
            try:
                # Try to read all sheets
                excel_file = pd.ExcelFile(file_path)
                
                # Write all sheets to the new file
                with pd.ExcelWriter(new_file, engine='openpyxl') as writer:
                    # First copy all existing sheets except the one we're adding
                    for sheet in excel_file.sheet_names:
                        if sheet != sheet_name:
                            data = pd.read_excel(file_path, sheet_name=sheet)
                            data.to_excel(writer, sheet_name=sheet, index=False)
                    
                    # Then add our new sheet
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                print(f"New file created with updated data: {new_file}")
                print(f"Original file preserved: {file_path}")
            except Exception as e:
                print(f"Error copying existing sheets: {e}")
                print("Creating new file with just the new sheet...")
                df.to_excel(new_file, sheet_name=sheet_name, index=False)
        else:
            # Just create a new file
            df.to_excel(new_file, sheet_name=sheet_name, index=False)
            print(f"New file created: {new_file}")
        
        return True
        
    except Exception as e:
        print(f"Error creating Excel file: {e}")
        return False


# uncomment to use the function:
# add_df_to_excel(match_df)


