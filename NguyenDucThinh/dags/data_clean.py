import pandas as pd
import os

def clean_data():
    input_path = "/opt/airflow/data/crawl_data/nguyenducthinh_fpt.json"
    output_dir = "/opt/airflow/data/clean_data"

    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_json(input_path, lines=True, encoding="utf-8")
    
    df_hcm = df[df['address'].str.contains('Hồ Chí Minh', case=False, na=False)]
    
    now = pd.Timestamp.now()
    year = now.year
    month = now.month
    day = now.day
    output_json_path = os.path.join(output_dir, f"{year}/{month}/{day}/nguyenducthinh_filtered.json")
    os.makedirs(os.path.dirname(output_json_path), exist_ok=True)
    df_hcm.to_json(output_json_path, orient="records", force_ascii=False, lines=True)
    
    csv_output_path = os.path.join(output_dir, f"{year}/{month}/{day}/nguyenducthinh_filtered.csv")
    os.makedirs(os.path.dirname(csv_output_path), exist_ok=True)
    df_hcm.to_csv(csv_output_path, index=False, encoding="utf-8")
