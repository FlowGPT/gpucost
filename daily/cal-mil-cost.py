import dbutils
from dbutils import GPUHourCost
from prom_utils import query_prometheus_with_custom_range
from datetime import datetime, timedelta

hourly_gpu_cost_ids2cluster={
    'eris-12b-ex': 'k8s/exabits-h100/dcgm-exporter',
    'eris-12b-ex-ca': 'k8s/exabits-ca/dcgm-exporter',
}

yesterday = datetime.now() - timedelta(days=1)
yesterday = yesterday.strftime("%Y-%m-%d")
today = datetime.now().strftime("%Y-%m-%d")

matched_records, unmatched_ids = dbutils.get_matched_records(yesterday)
print(f"Query Records for {yesterday}:")

for record in matched_records:
    print(f"Matched Record: {record}")
if len(unmatched_ids) > 0:
    print(f"Unmatched IDs: {unmatched_ids}")

pg_conn = dbutils.get_pgdb_connection()

for record in matched_records:
    id = record.id
    gpudatas = dbutils.get_by_cluster(pg_conn, id)
    if len(gpudatas) > 1:
        raise ValueError(f"Error: GPU data for ID {id} is not unique.")
    if len(gpudatas) == 0:
        print(f"Warning: No GPU data found for ID {id}.")
        continue
    gpuhourdata: GPUHourCost = gpudatas[0]
    if id in hourly_gpu_cost_ids2cluster.keys():
        prom_cluster = hourly_gpu_cost_ids2cluster[id]
        gpu_hour_nums_list = query_prometheus_with_custom_range(yesterday,today,job=prom_cluster)
        print(f"GPU hour nums list for ID {id}: {gpu_hour_nums_list}")
        
    else:
        gpu_cost = gpuhourdata.price * gpuhourdata.card_num * 24
    input_mil_cost = gpu_cost / (record.input_tokens + 5 * record.output_tokens) * 1000000
    input_mil_cost = round(input_mil_cost, 3)
    output_mil_cost = round(input_mil_cost * 5, 3)
    print(f"ID: {id}, gpu cost: {gpu_cost}, Input MIL Cost: {input_mil_cost}, Output MIL Cost: {output_mil_cost}")
    dbutils.update_providercost_table(pg_conn, id, input_mil_cost, output_mil_cost)

pg_conn.close()
