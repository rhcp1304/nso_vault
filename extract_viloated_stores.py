import pandas as pd
import re
import os


def extract_rent_values(text):
    """
    Cleans text and extracts all significant numeric values.
    Filters out small numbers (1, 2, 3) which are usually 'Years'.
    """
    if pd.isna(text) or text == '-':
        return []

    # Remove currency symbols and commas
    clean_text = str(text).replace(',', '').replace('₹', '').replace('$', '')

    # Find all numbers (including decimals)
    nums = re.findall(r'(\d+(?:\.\d+)?)', clean_text)

    # Convert to float and filter out numbers < 500 (likely years, percentages, or dates)
    valid_nums = [float(n) for n in nums if float(n) > 500]
    return valid_nums


def process_rent_violations(input_csv, output_csv):
    if not os.path.exists(input_csv):
        print(f"Error: {input_csv} not found.")
        return

    print(f"Reading {input_csv}...")
    df = pd.read_csv(input_csv)

    refined_data = []

    for index, row in df.iterrows():
        # 1. Parse Ceiling Rent
        # If it's a range (e.g. 100000-110000), we take the higher value as the approved limit
        ceil_values = extract_rent_values(row['Ceiling Rent (Absolute)'])
        approved_limit = max(ceil_values) if ceil_values else 0.0

        # 2. Parse Actual Rent
        # Extracts all escalations (e.g. 1st yr, 2nd yr) and finds the highest (Peak)
        actual_values = extract_rent_values(row['Actual Master Rent'])
        peak_actual = max(actual_values) if actual_values else 0.0

        # 3. Calculate Variance
        variance = peak_actual - approved_limit
        is_violation = "YES" if variance > 0 else "NO"

        # Add to collection
        refined_data.append({
            'Presentation Date': row.get('Presentation Date', ''),
            'Store Code': row.get('Store Code', 'TBD'),
            'Store Name': row.get('Store Name', ''),
            'Address': row.get('Address', ''),
            'Latitude': row.get('Latitude', ''),
            'Longitude': row.get('Longitude', ''),
            'Approved Ceiling': approved_limit,
            'Peak Actual Rent': peak_actual,
            'Variance': variance,
            'Is Violation': is_violation,
            'Original Ceiling Text': row['Ceiling Rent (Absolute)'],
            'Original Actual Rent Text': row['Actual Master Rent']
        })

    # Create DataFrame
    result_df = pd.DataFrame(refined_data)

    # Optional: Filter to show only violations
    # result_df = result_df[result_df['Is Violation'] == "YES"]

    # Sort by the largest variance (biggest violations first)
    result_df = result_df.sort_values(by='Variance', ascending=False)

    # Save to CSV
    result_df.to_csv(output_csv, index=False)
    print(f"Successfully processed {len(result_df)} records.")
    print(f"Refined file saved as: {output_csv}")


if __name__ == "__main__":
    # Settings
    INPUT_FILE = 'Rent_Violation_Analysis.csv'
    OUTPUT_FILE = 'Refined_Rent_Analysis.csv'

    process_rent_violations(INPUT_FILE, OUTPUT_FILE)