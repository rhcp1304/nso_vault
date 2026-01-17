import pandas as pd

# 1. SETUP
remarks_file = 'Final_Rental_Amounts_Absolute.csv'
master_file = 'All-Stores-Master-data-4th Jan-26.xlsx'
output_file = 'Rent_Violation_Analysis.csv'


def get_clean_master_df(file_path):
    """Detects header row in Master sheet and reads data."""
    preview = pd.read_excel(file_path, sheet_name='Master', nrows=20, header=None)
    for idx, row in preview.iterrows():
        row_values = [str(val).strip().lower() for val in row.values]
        if 'latitude' in row_values or 'lat' in row_values:
            return pd.read_excel(file_path, sheet_name='Master', skiprows=idx)
    return pd.read_excel(file_path, sheet_name='Master')


try:
    print("Reading files...")
    df_rem = pd.read_csv(remarks_file)
    df_mas = get_clean_master_df(master_file)

    # 2. COORDINATE CLEANING (Remove '-' and non-numeric values)
    # This ensures we don't join on empty or dash values
    df_rem['lat_clean'] = pd.to_numeric(df_rem['LATITUDE'], errors='coerce')
    df_rem['lon_clean'] = pd.to_numeric(df_rem['LONGITUDE'], errors='coerce')
    df_mas['lat_clean'] = pd.to_numeric(df_mas['Latitude'], errors='coerce')
    df_mas['lon_clean'] = pd.to_numeric(df_mas['Longitude'], errors='coerce')

    # Drop rows where coordinates are missing or invalid
    df_rem_valid = df_rem.dropna(subset=['lat_clean', 'lon_clean'])
    df_mas_valid = df_mas.dropna(subset=['lat_clean', 'lon_clean'])

    # 3. STANDARDIZE KEYS (Round to 5 decimals to ensure exact matching)
    df_rem_valid['join_key_lat'] = df_rem_valid['lat_clean'].round(5).astype(str)
    df_rem_valid['join_key_lon'] = df_rem_valid['lon_clean'].round(5).astype(str)
    df_mas_valid['join_key_lat'] = df_mas_valid['lat_clean'].round(5).astype(str)
    df_mas_valid['join_key_lon'] = df_mas_valid['lon_clean'].round(5).astype(str)

    # 4. JOIN
    print(f"Joining {len(df_rem_valid)} records with Master data...")
    merged = pd.merge(
        df_rem_valid,
        df_mas_valid,
        on=['join_key_lat', 'join_key_lon'],
        how='inner'
    )

    # 5. SELECT REQUESTED COLUMNS
    # Note: 'ST Code' is from remarks, 'Store Address' and 'Rent' from Master
    column_mapping = {
        'Presentation Date': 'Presentation Date',
        'ST Code': 'Store Code',
        'Store Name_y': 'Store Name',
        'Store Address': 'Address',
        'lat_clean_x': 'Latitude',
        'lon_clean_x': 'Longitude',
        'rental_ceiling_absolute': 'Ceiling Rent (Absolute)',
        'Rent': 'Actual Master Rent'
    }

    # Ensure columns exist before selecting
    available_cols = [c for c in column_mapping.keys() if c in merged.columns]
    final_df = merged[available_cols].rename(columns=column_mapping)

    # 6. SAVE
    final_df.to_csv(output_file, index=False)

    print(f"\n✅ SUCCESS!")
    print(f"Rows matched and saved: {len(final_df)}")
    print(f"Columns included: {list(final_df.columns)}")

except Exception as e:
    print(f"❌ ERROR: {e}")