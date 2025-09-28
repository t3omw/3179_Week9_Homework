# import pandas as pd
# import numpy as np

# # --- Configuration ---
# OBSERVED_CSV_FILE = 'malaysia_rainfall_2000_2021.csv'
# PROJECTED_CSV_FILE = 'malaysia_daily_rainfall_2014_2020.csv'
# FINAL_OUTPUT_CSV_FILE = 'malaysia_avg_annual_rainfall_final.csv'

# # Target time period for all averages
# TARGET_START_YEAR = 2014
# TARGET_END_YEAR = 2020

# # Column names in the observed dataset
# OBSERVED_RAINFALL_COL = 'Total Rainfall in millimetres'
# OBSERVED_STATE_COL = 'State'
# OBSERVED_YEAR_COL = 'Year'

# # Column names in the projected dataset
# PROJECTED_STATE_COL = 'State'
# PROJECTED_YEAR_COL = 'Year'
# PROJECTED_RAINFALL_COL = 'Rainfall (mm)'

# # --- Master list of all 16 administrative units (from your GeoJSON) ---
# GEOJSON_ADMIN_UNITS = {
#     'Johor', 'Kedah', 'Kelantan', 'Kuala Lumpur', 'Labuan', 'Melaka',
#     'Negeri Sembilan', 'Pahang', 'Perak', 'Perlis', 'Pulau Pinang',
#     'Putrajaya', 'Sabah', 'Sarawak', 'Selangor', 'Terengganu'
# }
# MALAYSIA_STATES = {
#     'Johor', 'Kedah', 'Kelantan', 'Melaka', 'Negeri Sembilan', 'Pahang',
#     'Perak', 'Perlis', 'Pulau Pinang', 'Sabah', 'Sarawak', 'Selangor',
#     'Terengganu'
# }
# FEDERAL_TERRITORIES = {'Kuala Lumpur', 'Labuan', 'Putrajaya'}


# # --- Utility function for common processing steps ---
# def process_rainfall_data(df, rainfall_col, state_col, year_col, renames=None):
#     """Cleans, filters by year, renames states, and aggregates data."""
#     df_processed = df.copy()

#     # Clean rainfall column
#     df_processed[rainfall_col] = df_processed[rainfall_col].replace(['-', 'Def.', '2.819.6'], np.nan)
#     df_processed[rainfall_col] = pd.to_numeric(df_processed[rainfall_col], errors='coerce')
#     df_processed.dropna(subset=[rainfall_col], inplace=True)

#     # Ensure Year is numeric and filter
#     df_processed[year_col] = pd.to_numeric(df_processed[year_col], errors='coerce')
#     df_processed.dropna(subset=[year_col], inplace=True)
#     df_processed[year_col] = df_processed[year_col].astype(int)
#     df_processed = df_processed[
#         (df_processed[year_col] >= TARGET_START_YEAR) &
#         (df_processed[year_col] <= TARGET_END_YEAR)
#     ].copy()

#     # Apply custom state renames
#     if renames:
#         df_processed[state_col] = df_processed[state_col].replace(renames)

#     # Aggregate to annual sum per administrative unit
#     annual_sums_by_unit_year = df_processed.groupby([state_col, year_col])[rainfall_col].sum().reset_index()

#     # Calculate overall average annual rainfall per administrative unit for the target period
#     final_avg_rainfall_by_unit = annual_sums_by_unit_year.groupby(state_col)[rainfall_col].mean().reset_index()
#     final_avg_rainfall_by_unit.rename(columns={rainfall_col: 'Average_Annual_Rainfall_mm'}, inplace=True)

#     return final_avg_rainfall_by_unit

# # --- Step 1: Process Observed Data (2000-2021.csv, filtered to 2014-2020) ---
# print(f"--- Processing Observed Data: '{OBSERVED_CSV_FILE}' for {TARGET_START_YEAR}-{TARGET_END_YEAR} ---")
# try:
#     df_observed = pd.read_csv(OBSERVED_CSV_FILE)
# except FileNotFoundError:
#     print(f"Error: '{OBSERVED_CSV_FILE}' not found.")
#     exit()

# # Renames for observed data's State column
# # Only Labuan (from Wilayah Persekutuan Labuan) is typically in this file and needs renaming
# observed_renames = {
#     'Wilayah Persekutuan Labuan': 'Labuan',
# }

# df_observed_processed = process_rainfall_data(
#     df_observed, OBSERVED_RAINFALL_COL, OBSERVED_STATE_COL, OBSERVED_YEAR_COL, observed_renames
# )
# print(f"Processed observed data shape: {df_observed_processed.shape}")
# print("Observed data head:\n", df_observed_processed.head())


# # --- Step 2: Process Projected Data (daily_projected.csv, filtered to 2014-2020) ---
# print(f"\n--- Processing Projected Data: '{PROJECTED_CSV_FILE}' for {TARGET_START_YEAR}-{TARGET_END_YEAR} ---")
# try:
#     df_projected = pd.read_csv(PROJECTED_CSV_FILE)
# except FileNotFoundError:
#     print(f"Error: '{PROJECTED_CSV_FILE}' not found.")
#     exit()

# # Custom exclusion for projected data: Ignore 'Selangor-Wilayah'
# initial_projected_rows = df_projected.shape[0]
# df_projected = df_projected[df_projected[PROJECTED_STATE_COL] != 'Selangor-Wilayah'].copy()
# print(f"Ignored 'Selangor-Wilayah' from projected dataset. {initial_projected_rows - df_projected.shape[0]} rows removed.")

# # Renames for projected data's State column to match GeoJSON standards
# projected_renames = {
#     'NSembilan': 'Negeri Sembilan',
#     'Penang': 'Pulau Pinang', # Rename 'Penang' to 'Pulau Pinang' for consistency
#     'W.P. Kuala Lumpur': 'Kuala Lumpur', # Standardize FT name
#     'W.P. Putrajaya': 'Putrajaya',       # Standardize FT name
#     'W.P. Labuan': 'Labuan',              # Standardize FT name
#     # Other states like 'Johor', 'Kedah', 'Selangor', 'Sabah', 'Sarawak', 'Terengganu', 'Pahang', 'Perak', 'Perlis'
#     # are assumed consistent or are handled by the replacement logic.
# }

# df_projected_processed = process_rainfall_data(
#     df_projected, PROJECTED_RAINFALL_COL, PROJECTED_STATE_COL, PROJECTED_YEAR_COL, projected_renames
# )
# print(f"Processed projected data shape: {df_projected_processed.shape}")
# print("Projected data head:\n", df_projected_processed.head())


# # --- Step 3: Combine Data, Prioritizing Observed, then adding proxies for KL/Putrajaya ---
# print("\n--- Combining Processed Datasets (Observed prioritized, then Projected, then Proxies) ---")

# # Start with observed data
# df_final_combined = df_observed_processed.set_index('State')

# # Update from projected data for any administrative units missing in observed.
# # This fills in 'Negeri Sembilan', 'Kuala Lumpur', 'Putrajaya' (if they existed in projected) etc.
# df_final_combined = df_final_combined.combine_first(df_projected_processed.set_index('State'))

# # Extract Selangor's average for proxying BEFORE adding KL/Putrajaya
# # This average is from the already combined and prioritized data (2014-2020)
# if 'Selangor' in df_final_combined.index:
#     selangor_avg_rainfall = df_final_combined.loc['Selangor', 'Average_Annual_Rainfall_mm']
#     print(f"Extracted Selangor's average rainfall ({TARGET_START_YEAR}-{TARGET_END_YEAR}) for proxying: {selangor_avg_rainfall:.2f} mm")
# else:
#     print("WARNING: 'Selangor' data not found after initial combination. Cannot set proxy for KL/Putrajaya.")
#     selangor_avg_rainfall = np.nan # Set to NaN if Selangor itself is missing

# # Check if Kuala Lumpur is still missing and add proxy if so
# if 'Kuala Lumpur' not in df_final_combined.index:
#     if not np.isnan(selangor_avg_rainfall):
#         kuala_lumpur_row = pd.DataFrame([{'State': 'Kuala Lumpur', 'Average_Annual_Rainfall_mm': selangor_avg_rainfall}])
#         df_final_combined = pd.concat([df_final_combined.reset_index(), kuala_lumpur_row], ignore_index=True).set_index('State')
#         print(f"Added Kuala Lumpur proxy data using Selangor's average: {selangor_avg_rainfall:.2f} mm")
#     else:
#         print("Kuala Lumpur remains missing as Selangor's proxy data is unavailable.")

# # Check if Putrajaya is still missing and add proxy if so
# if 'Putrajaya' not in df_final_combined.index:
#     if not np.isnan(selangor_avg_rainfall):
#         putrajaya_row = pd.DataFrame([{'State': 'Putrajaya', 'Average_Annual_Rainfall_mm': selangor_avg_rainfall}])
#         df_final_combined = pd.concat([df_final_combined.reset_index(), putrajaya_row], ignore_index=True).set_index('State')
#         print(f"Added Putrajaya proxy data using Selangor's average: {selangor_avg_rainfall:.2f} mm")
#     else:
#         print("Putrajaya remains missing as Selangor's proxy data is unavailable.")

# df_final_combined = df_final_combined.reset_index() # Bring 'State' back as a column

# print(f"Final combined data shape: {df_final_combined.shape}")
# print("\nFinal combined data head:\n", df_final_combined.head())
# print("\nFinal combined data tail:\n", df_final_combined.tail())


# # --- Step 4: Final Cross-check with GeoJSON Administrative Units ---
# print("\n--- Final Cross-checking with GeoJSON Administrative Units ---")
# csv_admin_units = set(df_final_combined['State'].unique())

# missing_in_csv = GEOJSON_ADMIN_UNITS - csv_admin_units
# missing_in_geojson = csv_admin_units - GEOJSON_ADMIN_UNITS

# print(f"Number of unique administrative units in final processed CSV: {len(csv_admin_units)}")
# print(f"Number of unique administrative units in GeoJSON: {len(GEOJSON_ADMIN_UNITS)}")

# if not missing_in_csv and not missing_in_geojson:
#     print("\nSUCCESS: All administrative unit names in CSV match GeoJSON exactly. No missing or extra units. Map should render fully!")
# else:
#     if missing_in_csv:
#         print(f"\nCRITICAL: The following administrative units are in your GeoJSON but NOT in your final processed CSV data:")
#         for unit in sorted(list(missing_in_csv)):
#             classification = ""
#             if unit in MALAYSIA_STATES: classification = "State"
#             elif unit in FEDERAL_TERRITORIES: classification = "Federal Territory"
#             else: classification = "Unknown Classification"
#             print(f"- {unit} ({classification})")
#         print("\nThese areas will likely appear UNCOLORED on your map, leading to a potential penalty.")
#         print("Please review your data sources and the preprocessing logic.")
#     if missing_in_geojson:
#         print(f"\nWARNING: The following administrative units are in your final processed CSV data but NOT in your GeoJSON:")
#         for unit in sorted(list(missing_in_geojson)):
#             print(f"- {unit}")
#         print("This is unusual. Please double-check your GeoJSON file's 'NAME_1' attribute values against the 'GEOJSON_ADMIN_UNITS' list in this script.")

# # --- Save Processed Data ---
# df_final_combined.to_csv(FINAL_OUTPUT_CSV_FILE, index=False)
# print(f"\nFinal processed data saved to '{FINAL_OUTPUT_CSV_FILE}'")

import pandas as pd
import numpy as np

# --- Configuration ---
OBSERVED_CSV_FILE = 'malaysia_rainfall_2000_2021.csv'
PROJECTED_CSV_FILE = 'malaysia_daily_rainfall_2014_2020.csv'
AREA_CSV_FILE = 'malaysia_state_area.csv'
FINAL_OUTPUT_CSV_FILE = 'malaysia_avg_annual_rainfall_final_normalized.csv'

# Target time period for all averages
TARGET_START_YEAR = 2014
TARGET_END_YEAR = 2020

# Column names in the observed dataset
OBSERVED_RAINFALL_COL = 'Total Rainfall in millimetres'
OBSERVED_STATE_COL = 'State'
OBSERVED_YEAR_COL = 'Year'

# Column names in the projected dataset
PROJECTED_STATE_COL = 'State'
PROJECTED_YEAR_COL = 'Year'
PROJECTED_RAINFALL_COL = 'Rainfall (mm)'

# --- Master list of all 16 administrative units (from your GeoJSON) ---
GEOJSON_ADMIN_UNITS = {
    'Johor', 'Kedah', 'Kelantan', 'Kuala Lumpur', 'Labuan', 'Melaka',
    'Negeri Sembilan', 'Pahang', 'Perak', 'Perlis', 'Pulau Pinang',
    'Putrajaya', 'Sabah', 'Sarawak', 'Selangor', 'Terengganu'
}
MALAYSIA_STATES = {
    'Johor', 'Kedah', 'Kelantan', 'Melaka', 'Negeri Sembilan', 'Pahang',
    'Perak', 'Perlis', 'Pulau Pinang', 'Sabah', 'Sarawak', 'Selangor',
    'Terengganu'
}
FEDERAL_TERRITORIES = {'Kuala Lumpur', 'Labuan', 'Putrajaya'}


# --- Utility function for common processing steps ---
def process_rainfall_data(df, rainfall_col, state_col, year_col, renames=None):
    """Cleans, filters by year, renames states, and aggregates data."""
    df_processed = df.copy()

    # Clean rainfall column
    df_processed[rainfall_col] = df_processed[rainfall_col].replace(['-', 'Def.', '2.819.6'], np.nan)
    df_processed[rainfall_col] = pd.to_numeric(df_processed[rainfall_col], errors='coerce')
    df_processed.dropna(subset=[rainfall_col], inplace=True)

    # Ensure Year is numeric and filter
    df_processed[year_col] = pd.to_numeric(df_processed[year_col], errors='coerce')
    df_processed.dropna(subset=[year_col], inplace=True)
    df_processed[year_col] = df_processed[year_col].astype(int)
    df_processed = df_processed[
        (df_processed[year_col] >= TARGET_START_YEAR) &
        (df_processed[year_col] <= TARGET_END_YEAR)
    ].copy()

    # Apply custom state renames
    if renames:
        df_processed[state_col] = df_processed[state_col].replace(renames)

    # Aggregate to annual sum per administrative unit
    annual_sums_by_unit_year = df_processed.groupby([state_col, year_col])[rainfall_col].sum().reset_index()

    # Calculate overall average annual rainfall per administrative unit for the target period
    final_avg_rainfall_by_unit = annual_sums_by_unit_year.groupby(state_col)[rainfall_col].mean().reset_index()
    final_avg_rainfall_by_unit.rename(columns={rainfall_col: 'Average_Annual_Rainfall_mm'}, inplace=True)

    return final_avg_rainfall_by_unit

# --- Step 1: Process Observed Data (2000-2021.csv, filtered to 2014-2020) ---
print(f"--- Processing Observed Data: '{OBSERVED_CSV_FILE}' for {TARGET_START_YEAR}-{TARGET_END_YEAR} ---")
try:
    df_observed = pd.read_csv(OBSERVED_CSV_FILE)
except FileNotFoundError:
    print(f"Error: '{OBSERVED_CSV_FILE}' not found.")
    exit()

observed_renames = {'Wilayah Persekutuan Labuan': 'Labuan'}
df_observed_processed = process_rainfall_data(
    df_observed, OBSERVED_RAINFALL_COL, OBSERVED_STATE_COL, OBSERVED_YEAR_COL, observed_renames
)
print(f"Processed observed data shape: {df_observed_processed.shape}")
print("Observed data head:\n", df_observed_processed.head())


# --- Step 2: Process Projected Data (malaysia_daily_rainfall_2014_2020.csv, filtered to 2014-2020) ---
print(f"\n--- Processing Projected Data: '{PROJECTED_CSV_FILE}' for {TARGET_START_YEAR}-{TARGET_END_YEAR} ---")
try:
    df_projected = pd.read_csv(PROJECTED_CSV_FILE)
except FileNotFoundError:
    print(f"Error: '{PROJECTED_CSV_FILE}' not found.")
    exit()

initial_projected_rows = df_projected.shape[0]
df_projected = df_projected[df_projected[PROJECTED_STATE_COL] != 'Selangor-Wilayah'].copy()
print(f"Ignored 'Selangor-Wilayah' from projected dataset. {initial_projected_rows - df_projected.shape[0]} rows removed.")

projected_renames = {
    'NSembilan': 'Negeri Sembilan',
    'Penang': 'Pulau Pinang',
    'W.P. Kuala Lumpur': 'Kuala Lumpur',
    'W.P. Putrajaya': 'Putrajaya',
    'W.P. Labuan': 'Labuan',
}
df_projected_processed = process_rainfall_data(
    df_projected, PROJECTED_RAINFALL_COL, PROJECTED_STATE_COL, PROJECTED_YEAR_COL, projected_renames
)
print(f"Processed projected data shape: {df_projected_processed.shape}")
print("Projected data head:\n", df_projected_processed.head())


# --- Step 3: Combine Data, Prioritizing Observed ---
print("\n--- Combining Processed Datasets (Observed prioritized, then Projected) ---")

df_final_combined = df_observed_processed.set_index('State')
df_final_combined = df_final_combined.combine_first(df_projected_processed.set_index('State'))
df_final_combined = df_final_combined.reset_index()


# --- Step 4: Load Area Data and Normalize Rainfall ---
print(f"\n--- Loading Area Data: '{AREA_CSV_FILE}' and Normalizing Rainfall ---")
try:
    df_area = pd.read_csv(AREA_CSV_FILE)
    print(f"Successfully loaded '{AREA_CSV_FILE}'. Shape: {df_area.shape}")
except FileNotFoundError:
    print(f"Error: '{AREA_CSV_FILE}' not found. Please create this file with 'State' and 'Area_sqkm' columns.")
    exit()

# Ensure consistent state names in area data before merging
df_area['State'] = df_area['State'].replace({
    'Penang': 'Pulau Pinang',
    'W.P. Kuala Lumpur': 'Kuala Lumpur',
    'W.P. Putrajaya': 'Putrajaya',
    'W.P. Labuan': 'Labuan',
    'NSembilan': 'Negeri Sembilan'
})

# Merge rainfall data with area data
df_final_normalized = pd.merge(df_final_combined, df_area, on='State', how='left')

# Handle cases where area data might be missing (shouldn't happen if AREA_CSV_FILE is complete)
if df_final_normalized['Area_sqkm'].isnull().any():
    print("WARNING: Some administrative units are missing area data. They will not be normalized.")
    # For a clean output, we'll drop rows where area is missing for normalization purposes
    df_final_normalized.dropna(subset=['Area_sqkm'], inplace=True)


# --- Extract Selangor's Normalized Rainfall for Proxying KL/Putrajaya ---
# Calculate normalized rainfall for all regions first
df_final_normalized['Average_Annual_Rainfall_per_sqkm'] = df_final_normalized['Average_Annual_Rainfall_mm'] / df_final_normalized['Area_sqkm']

# Get Selangor's normalized rainfall
if 'Selangor' in df_final_normalized['State'].values:
    selangor_avg_rainfall_per_sqkm = df_final_normalized[
        df_final_normalized['State'] == 'Selangor'
    ]['Average_Annual_Rainfall_per_sqkm'].iloc[0]
    print(f"Extracted Selangor's average rainfall per sqkm ({TARGET_START_YEAR}-{TARGET_END_YEAR}) for proxying: {selangor_avg_rainfall_per_sqkm:.4f} mm/km²")
else:
    print("WARNING: 'Selangor' data not found after normalization. Setting proxy rainfall per sqkm to 0 for missing FTs.")
    selangor_avg_rainfall_per_sqkm = 0 # Fallback if Selangor itself somehow disappears


# --- Step 5: Add/Update Proxy Data for Kuala Lumpur and Putrajaya (using normalized proxy) ---
print("\n--- Adding/Updating Proxy Data for Federal Territories (using normalized proxy) ---")

# Function to safely update or add row
def update_or_add_proxy(df, state_name, proxy_value):
    if state_name in df['State'].values:
        # Update existing row
        df.loc[df['State'] == state_name, 'Average_Annual_Rainfall_per_sqkm'] = proxy_value
        print(f"Updated {state_name} with Selangor's normalized proxy: {proxy_value:.4f} mm/km²")
    else:
        # Add new row
        new_row = pd.DataFrame([{'State': state_name, 'Average_Annual_Rainfall_per_sqkm': proxy_value}])
        df = pd.concat([df, new_row], ignore_index=True)
        print(f"Added {state_name} with Selangor's normalized proxy: {proxy_value:.4f} mm/km²")
    return df

# Apply proxy for Kuala Lumpur
df_final_normalized = update_or_add_proxy(df_final_normalized, 'Kuala Lumpur', selangor_avg_rainfall_per_sqkm)

# Apply proxy for Putrajaya
df_final_normalized = update_or_add_proxy(df_final_normalized, 'Putrajaya', selangor_avg_rainfall_per_sqkm)


# Drop the original non-normalized rainfall and area columns if you only want the normalized data
# This has to be done after normalization and proxy assignment
df_final_normalized.drop(columns=['Average_Annual_Rainfall_mm', 'Area_sqkm'], inplace=True, errors='ignore')


# --- Step 6: Final Cross-check with GeoJSON Administrative Units ---
print("\n--- Final Cross-checking with GeoJSON Administrative Units ---")
csv_admin_units = set(df_final_normalized['State'].unique())

missing_in_csv = GEOJSON_ADMIN_UNITS - csv_admin_units
missing_in_geojson = csv_admin_units - GEOJSON_ADMIN_UNITS

print(f"Number of unique administrative units in final processed CSV: {len(csv_admin_units)}")
print(f"Number of unique administrative units in GeoJSON: {len(GEOJSON_ADMIN_UNITS)}")

if not missing_in_csv and not missing_in_geojson:
    print("\nSUCCESS: All administrative unit names in CSV match GeoJSON exactly. No missing or extra units. Map should render fully!")
else:
    if missing_in_csv:
        print(f"\nCRITICAL: The following administrative units are in your GeoJSON but NOT in your final processed CSV data:")
        for unit in sorted(list(missing_in_csv)):
            classification = ""
            if unit in MALAYSIA_STATES: classification = "State"
            elif unit in FEDERAL_TERRITORIES: classification = "Federal Territory"
            else: classification = "Unknown Classification"
            print(f"- {unit} ({classification})")
        print("\nThese areas will likely appear UNCOLORED on your map, leading to a potential penalty.")
        print("Please review your data sources and the preprocessing logic, especially your 'malaysia_areas.csv'.")
    if missing_in_geojson:
        print(f"\nWARNING: The following administrative units are in your final processed CSV data but NOT in your GeoJSON:")
        for unit in sorted(list(missing_in_geojson)):
            print(f"- {unit}")
        print("This is unusual. Please double-check your GeoJSON file's 'NAME_1' attribute values against the 'GEOJSON_ADMIN_UNITS' list in this script.")


# --- Save Processed Data ---
df_final_normalized.to_csv(FINAL_OUTPUT_CSV_FILE, index=False)
print(f"\nFinal processed (normalized) data saved to '{FINAL_OUTPUT_CSV_FILE}'")
print("\nFinal normalized data head:")
print(df_final_normalized.head())
print("\nFinal normalized data tail:")
print(df_final_normalized.tail())
print(df_final_normalized)