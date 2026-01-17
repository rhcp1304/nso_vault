import pandas as pd
import re
import json
import time
import os
from google import genai

# 1. SETUP
api_key = os.environ.get('GEMINI_API_KEY')
client = genai.Client(api_key=api_key)

MODEL_NAME = 'gemini-2.0-flash'
FILE_PATH = 'Consolidated_Remarks_List.xlsx'
OUTPUT_FILE = 'Final_Rental_Amounts_Absolute.csv'


def convert_to_absolute(val_str):
    """
    Converts '1.20' to '120000' and '0.60-0.70' to '60000-70000'
    """
    if not val_str or val_str == "null":
        return None

    try:
        # Handle ranges (e.g., "0.60-0.70")
        if '-' in str(val_str):
            parts = str(val_str).split('-')
            abs_parts = [str(int(float(p.strip()) * 100000)) for p in parts]
            return "-".join(abs_parts)

        # Handle single values (e.g., "1.20")
        return str(int(float(val_str) * 100000))
    except:
        return val_str  # Return as is if conversion fails


def process_batches(df_column, batch_size=20):
    all_results = []

    for i in range(0, len(df_column), batch_size):
        batch = df_column.iloc[i:i + batch_size].tolist()
        expected_len = len(batch)

        prompt = f"""
        Extract rental ceiling amounts. Rules: 60k -> 0.60, 1.2 Lakhs -> 1.20, 80000 -> 0.80. Range -> 'min-max'.
        List: {batch}
        Return ONLY a JSON list of EXACTLY {expected_len} strings. Use null if no amount found.
        """

        try:
            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=prompt,
                config={'response_mime_type': 'application/json'}
            )

            batch_results = json.loads(response.text)

            # --- CRASH PROTECTION: Force length to match ---
            if isinstance(batch_results, list):
                if len(batch_results) > expected_len:
                    batch_results = batch_results[:expected_len]
                elif len(batch_results) < expected_len:
                    batch_results.extend([None] * (expected_len - len(batch_results)))
                all_results.extend(batch_results)
            else:
                all_results.extend([None] * expected_len)

            print(f"✅ Finished batch {i // batch_size + 1}...")
            time.sleep(0.5)

        except Exception as e:
            print(f"❌ Error in batch {i}: {e}")
            all_results.extend([None] * expected_len)

    return all_results


# 2. RUN
try:
    print(f"Loading {FILE_PATH}...")
    df = pd.read_excel(FILE_PATH)
    df['Remarks'] = df['Remarks'].fillna('').astype(str)

    # Filter for candidate rows
    mask = df['Remarks'].str.contains(r'\d', na=False)
    candidate_df = df[mask].copy()

    print(f"Processing {len(candidate_df)} rows with Gemini...")

    # Get Lakhs from Gemini
    lakhs_list = process_batches(candidate_df['Remarks'])

    # Final length check before assignment
    if len(lakhs_list) == len(candidate_df):
        candidate_df['rental_ceiling_lakhs'] = lakhs_list

        # --- NEW STEP: Convert to Absolute Numbers ---
        candidate_df['rental_ceiling_absolute'] = candidate_df['rental_ceiling_lakhs'].apply(convert_to_absolute)

        # Clean up: Remove rows where no amount was found
        final_df = candidate_df[candidate_df['rental_ceiling_absolute'].notna()].copy()

        # Save
        final_df.to_csv(OUTPUT_FILE, index=False)
        print(f"\n✨ SUCCESS! File saved to: {os.getcwd()}/{OUTPUT_FILE}")
        print("\nPreview of extracted data:")
        print(final_df[['Remarks', 'rental_ceiling_absolute']].head(10))
    else:
        print(f"⚠️ Length Mismatch Error: Input {len(candidate_df)} vs Output {len(lakhs_list)}")

except Exception as e:
    print(f"CRITICAL ERROR: {e}")