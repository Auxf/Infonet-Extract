import os
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import random
import re
from unidecode import unidecode

# Directory where you want to save company data
base_dir = "/Users/Desktop/YourFile"

# URL of the page to scrape: You need a URL with research and a filter that is already done. The scrapper will only get the results on page 1, you can check the page' number here : https://infonet.fr/recherche-entreprises/1
url = "https://infonet.fr/recherche-entreprises/1/P2FwZUNvZGVzPSZzZWN0b3JDb2Rlcz0mcG9zdGFsQ29kZXM9JnN0YXR1c2VzPUFjdGl2ZSZsZWdhbEZvcm1zPSZjaXRpZXM9Jm1pblNhbGVzPTI0MTIyMzU4MTc3JmluY2x1ZGVGb3JlaWduZXJzPTAmc29ydEJ5PWxhc3RfZmluYW5jaWFsX2Nsb3Npbmdfc2FsZXMmY3VzdG9tQ29sdW1uTmFtZT1zaXJldCZsaW1pdD0yNQ=="

#Different setups to avoid Captcha
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.2 Safari/605.1.15"
]

# Headers to avoid being blocked by the site
headers = {
    "Referer": "https://infonet.fr/",
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
    "User-Agent": random.choice(user_agents)
}

# Function to extract table data
def extract_table_data(table):
    rows = table.find_all("tr")
    table_data = []
    for row in rows:
        cols = row.find_all(["td", "th"])  # Include both headers and data
        cols = [ele.text.strip() for ele in cols]  # Clean the data
        table_data.append(cols)
    return table_data

# Function to convert values for table 1
def convert_value(value):
    if pd.isna(value) or value == '':
        return None  # Return None for NaN or empty values

    value_str = str(value).strip()  # Remove leading and trailing spaces

    # Handle numeric values with suffixes
    if value_str.endswith(" K"):
        return float(value_str[:-2].strip()) * 1000
    elif value_str.endswith(" M"):
        return float(value_str[:-2].strip()) * 1000000
    elif value_str.endswith(" Md"):
        return float(value_str[:-2].strip()) * 1000000000
    elif value_str.endswith(" K %"):
        number_part = value_str[:-4].strip()  # Exclude " K %"
        return float(number_part) * 1000 * 10  # Multiply by 1000 and then by 10
    elif value_str.endswith(" M %"):
        number_part = value_str[:-4].strip()  # Exclude " K %"
        return float(number_part) * 1000000 * 10  # Multiply by 1000 and then by 10
    elif value_str.endswith("%"):
        return float(value_str[:-1].strip()) / 100  # Convert percentage to decimal
    else:
        try:
            return float(value_str)  # Convert directly to float if no suffix
        except ValueError:
            return None  # Return None for any conversion errors

# Function to normalize titles
def normalize_title(title):
    # Remove accents, convert to lowercase, remove special characters, replace spaces with underscores
    normalized = unidecode(title).lower()
    normalized = re.sub(r'[^a-z0-9]', '_', normalized)  # Keep only alphanumeric and replace others with '_'
    normalized = re.sub(r'_+', '_', normalized)  # Replace multiple underscores with a single one
    normalized = normalized.strip('_')  # Remove leading or trailing underscores
    return normalized

# Function to process table data for tables 2, 3, and 4
def process_table_data(df, company_dir, table_name):
    split_data = [df.columns.tolist()]  # Add the header

    for index, row in df.iterrows():
        new_row = [normalize_title(row.iloc[0])]  # Normalize the first column (title)
        for cell in row[1:]:
            if pd.notna(cell):
                split_items = cell.split('\n')
                if len(split_items) > 0:
                    new_row.append(convert_value(split_items[0]))
                if len(split_items) > 1:
                    new_row.append(convert_value(split_items[-1]))
            else:
                new_row.append('')
        split_data.append(new_row)

    split_df = pd.DataFrame(split_data)

    # Remove the first row (original header)
    split_df = split_df.drop(index=0).reset_index(drop=True)

    # Adjust column titles
    years = df.columns[1:]  # Exclude the first column (titles)
    new_columns = [normalize_title(df.columns[0])]

    for year in years:
        year_str = str(year)
        new_columns.append(f"{year_str} Valeur")
        new_columns.append(f"{year_str} Variation")

    if len(new_columns) == split_df.shape[1]:
        split_df.columns = new_columns
    else:
        print(f"Warning: Column length mismatch! Expected {len(new_columns)}, got {split_df.shape[1]}")

    # Calculate the previous year's value using the variation
    last_value_col = split_df.columns[-2]
    last_variation_col = split_df.columns[-1]

    split_df['Previous Year Value'] = split_df[last_value_col] / (1 + split_df[last_variation_col])

    last_year_str = last_value_col.split(' ')[0]
    previous_year_str = str(int(last_year_str) - 1)

    split_df.insert(len(split_df.columns) - 1, f"{previous_year_str} Valeur", split_df['Previous Year Value'])
    split_df.drop(columns=['Previous Year Value'], inplace=True)

    # Save processed data to CSV
    split_df.to_csv(os.path.join(company_dir, f"{table_name}_processed.csv"), index=False)
    print(f"{table_name} processed and saved for {company_dir.split('/')[-1]}.")
    return split_df  # Return the processed DataFrame

# Scrape and process the data
response = requests.get(url, headers=headers)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a', class_='text-uppercase font-weight-bold stretched-link')

    for link in links:
        title = link.text.strip()
        href = link.get('href')
        company_url = "https://infonet.fr" + href

        print(f"Scraping data for company: {title}")

        company_dir = os.path.join(base_dir, title)
        os.makedirs(company_dir, exist_ok=True)

        response_company = requests.get(company_url, headers=headers)
        if response_company.status_code == 200:
            soup_company = BeautifulSoup(response_company.content, 'html.parser')

            # Extract tables
            table_1 = soup_company.find("table", class_="table table-hover border-bottom m-0")
            table_2 = soup_company.find("table", class_="table border-bottom mb-0")
            table_3 = soup_company.find_all("table", class_="table border-bottom mb-0")[1]
            table_4 = soup_company.find_all("table", class_="table border-bottom mb-0")[2]

            # Process and save table 1
            if table_1:
                table_1_data = extract_table_data(table_1)
                table_1_df = pd.DataFrame(table_1_data[1:], columns=table_1_data[0])
                
                # Apply conversion logic
                table_1_transformed = []
                for index, row in table_1_df.iterrows():
                    new_row = [normalize_title(row.iloc[0])]  # Normalize title in first column
                    for cell in row[1:]:
                        new_row.append(convert_value(cell))  # Convert and append the value
                    table_1_transformed.append(new_row)

                # Create DataFrame from transformed data
                split_table_1_df = pd.DataFrame(table_1_transformed)
                split_table_1_df.columns = table_1_df.columns  # Set column names
                split_table_1_df.to_csv(os.path.join(company_dir, "table_1_processed.csv"), index=False)
                print(f"Table 1 processed and saved for {title}.")
            
            # Process table_2 and save
            if table_2:
                table_2_data = extract_table_data(table_2)
                table_2_df = pd.DataFrame(table_2_data[1:], columns=table_2_data[0])  # Include the header row
                table_2_df.to_csv(os.path.join(company_dir, "table_2.csv"), index=False)
                processed_table_2 = process_table_data(table_2_df, company_dir, "table_2")  # Process table_2 data
            else:
                print(f"Table 2 not found for {title}.")

            # Process table_3 and save
            if table_3:
                table_3_data = extract_table_data(table_3)
                table_3_df = pd.DataFrame(table_3_data[1:], columns=table_3_data[0])  # Include the header row
                table_3_df.to_csv(os.path.join(company_dir, "table_3.csv"), index=False)
                processed_table_3 = process_table_data(table_3_df, company_dir, "table_3")  # Process table_3 data
            else:
                print(f"Table 3 not found for {title}.")

            # Process table_4 and save
            if table_4:
                table_4_data = extract_table_data(table_4)
                table_4_df = pd.DataFrame(table_4_data[1:], columns=table_4_data[0])  # Include the header row
                table_4_df.to_csv(os.path.join(company_dir, "table_4.csv"), index=False)
                processed_table_4 = process_table_data(table_4_df, company_dir, "table_4")  # Process table_4 data
            else:
                print(f"Table 4 not found for {title}.")

            # Drop columns that contain the word "variation"
            if 'processed_table_3' in locals():
                processed_table_3 = processed_table_3.loc[:, ~processed_table_3.columns.str.contains('variation', case=False)]
                processed_table_3.to_csv(os.path.join(company_dir, "table_3_final.csv"), index=False)
                print(f"Processed table 3 saved without 'variation' columns for {title}.")

            if 'processed_table_4' in locals():
                processed_table_4 = processed_table_4.loc[:, ~processed_table_4.columns.str.contains('variation', case=False)]
                processed_table_4.to_csv(os.path.join(company_dir, "table_4_final.csv"), index=False)
                print(f"Processed table 4 saved without 'variation' columns for {title}.")

            if 'processed_table_2' in locals():
                processed_table_2 = processed_table_2.loc[:, ~processed_table_2.columns.str.contains('variation', case=False)]
                processed_table_2.to_csv(os.path.join(company_dir, "table_2_final.csv"), index=False)
                print(f"Processed table 2 saved without 'variation' columns for {title}.")

            # Final merge of table_3_final and table_4_final
            if os.path.exists(os.path.join(company_dir, "table_3_final.csv")) and os.path.exists(os.path.join(company_dir, "table_4_final.csv")):
                final_table_3 = pd.read_csv(os.path.join(company_dir, "table_3_final.csv"))
                final_table_4 = pd.read_csv(os.path.join(company_dir, "table_4_final.csv"))

                # Rename first column of both tables to "Actif/Passif"
                final_table_3.rename(columns={final_table_3.columns[0]: "Actif/Passif"}, inplace=True)
                final_table_4.rename(columns={final_table_4.columns[0]: "Actif/Passif"}, inplace=True)

                # Combine both tables, stacking one on top of the other (row-wise)
                merged_final_table = pd.concat([final_table_3, final_table_4], ignore_index=True)

                # Save the merged table
                merged_final_table.to_csv(os.path.join(company_dir, "merged_table_3_4_final.csv"), index=False)
                print(f"Merged table 3 final and table 4 final saved for {title}.")
            else:
                print(f"One or both final tables (table 3 and table 4) not found for {title}.")

        else:
            print(f"Failed to retrieve the company page for {title}. Status code: {response_company.status_code}")

        # Sleep n seconds before moving to the next company
        sleep_time = random.randint(101, 301)
        print(f"Waiting for {sleep_time} seconds to avoid getting blocked...")
        time.sleep(sleep_time)
else:
    print(f"Failed to retrieve the main page. Status code: {response.status_code}")
