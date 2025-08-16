import dbutils
from dbutils import GPUHourCost
from datetime import datetime, timedelta

yesterday = datetime.now() - timedelta(days=1)
yesterday = yesterday.strftime("%Y-%m-%d")

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
    gpu_cost = gpuhourdata.price * gpuhourdata.card_num * 24
    input_mil_cost = gpu_cost / (record.input_tokens + 5 * record.output_tokens) * 1000000
    input_mil_cost = round(input_mil_cost, 3)
    output_mil_cost = round(input_mil_cost * 5, 3)
    print(f"ID: {id}, gpu cost: {gpu_cost}, Input MIL Cost: {input_mil_cost}, Output MIL Cost: {output_mil_cost}")
    dbutils.update_providercost_table(pg_conn, id, input_mil_cost, output_mil_cost)

pg_conn.close()
