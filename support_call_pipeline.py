from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import duckdb
import pandas as pd
import json
import os
from datetime import datetime, timedelta


DATA_DIR = "/usr/local/airflow/include/telephony_logs"
DUCKDB_PATH = "/usr/local/airflow/include/support_call.duckdb"
MYSQL_CONN_ID = "mysql_def"

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    start_date=datetime(2026, 3, 1),
    schedule="@hourly",
    catchup=False, 
    default_args=default_args,
    tags=['support', 'etl']
)
def support_call_enrichment():

    @task
    def detect_new_calls():
        last_time = Variable.get("last_loaded_call_time", default_var="2000-01-01 00:00:00")
        print(f"DEBUG: Looking for calls strictly after {last_time}")
        
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        sql = f"SELECT call_id FROM calls WHERE call_time > '{last_time}'"
        df_calls = mysql_hook.get_pandas_df(sql)
        
        if df_calls.empty:
            print("no new calls in DB")
            return []
        
        new_call_ids = df_calls['call_id'].tolist()
        
        print(f"Detected {len(new_call_ids)} new calls.")
        return new_call_ids

    @task
    def load_telephony_details(new_call_ids):
        if not new_call_ids:
            return []
            
        print(f"looking for JSON files in {DATA_DIR}")
        telephony_data = []
        rejected_count = 0
        
        for call_id in new_call_ids:
            file_path = os.path.join(DATA_DIR, f"{call_id}.json")
            
            if not os.path.exists(file_path):
                print(f"file missing for call_id {call_id}")
                rejected_count += 1
                continue
                
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    
                required_keys = {"call_id", "duration_sec", "short_description"}
                if not required_keys.issubset(data.keys()):
                    print(f"call_id {call_id} is missing required schema fields")
                    rejected_count += 1
                    continue
                
                if int(data['duration_sec']) < 0:
                    print(f"call_id {call_id} has negative duration ({data['duration_sec']}).")
                    rejected_count += 1
                    continue
                    
                telephony_data.append(data)
                
            except Exception as e:
                print(f"Failed to read or parse {file_path}: {e}")
                rejected_count += 1
                
        print(f"parsed {len(telephony_data)} valid JSONs. Rejected {rejected_count} files")
        return telephony_data

    @task
    def transform_and_load(new_call_ids, telephony_details):
        if not new_call_ids:
            print("No new data to transform and load")
            return

        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        ids_tuple = tuple(new_call_ids) if len(new_call_ids) > 1 else f"({new_call_ids[0]})"
        df_calls = mysql_hook.get_pandas_df(f"SELECT * FROM calls WHERE call_id IN {ids_tuple}")
        df_employees = mysql_hook.get_pandas_df("SELECT * FROM employees")
        
        df_telephony = pd.DataFrame(telephony_details) if telephony_details else pd.DataFrame(columns=['call_id', 'duration_sec', 'short_description'])

        df_final = df_calls.merge(df_employees, on='employee_id', how='left')
        df_final = df_final.merge(df_telephony, on='call_id', how='left')
        
        missing_employees = df_final['full_name'].isnull().sum()
        if missing_employees > 0:
            print(f"{missing_employees} calls assigned to unknown employees.")

        print(f"Attempting to load {len(df_final)} rows into DuckDB")

        conn = duckdb.connect(DUCKDB_PATH)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS support_call_enriched (
                call_id INTEGER PRIMARY KEY,
                employee_id INTEGER,
                call_time TIMESTAMP,
                phone VARCHAR,
                direction VARCHAR,
                status VARCHAR,
                full_name VARCHAR,
                team VARCHAR,
                role VARCHAR,
                hire_date DATE,
                duration_sec INTEGER,
                short_description VARCHAR
            )
        """)
        
        conn.execute("CREATE TEMP TABLE temp_stage AS SELECT * FROM df_final")
        conn.execute("""
            INSERT INTO support_call_enriched 
            SELECT * FROM temp_stage
            ON CONFLICT (call_id) DO UPDATE SET
                status = EXCLUDED.status,
                duration_sec = EXCLUDED.duration_sec,
                short_description = EXCLUDED.short_description
        """)
        conn.commit()
        conn.close()

        max_call_time = df_calls['call_time'].max()
        if pd.notnull(max_call_time):
            time_str = max_call_time.strftime('%Y-%m-%d %H:%M:%S')
            Variable.set("last_loaded_call_time", time_str)
            print(f"Watermark updated to {time_str}")

    call_ids = detect_new_calls()
    json_data = load_telephony_details(call_ids)
    transform_and_load(call_ids, json_data)

support_call_enrichment_dag = support_call_enrichment()