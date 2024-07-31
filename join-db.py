from pymongo import MongoClient
import psycopg2

# MongoDB connection
mongo_client = MongoClient("mongodb://username:password@localhost:27017")
mongo_db = mongo_client["newsdb"]
mongo_collection = mongo_db["subVoucher"]

# PostgreSQL connection
pg_conn = psycopg2.connect(
    dbname="db_name",
    user="db_user",
    password="db_password",
    host="db_host",
    port="db_port",
)
pg_cursor = pg_conn.cursor()
mongo_data = list(mongo_collection.find({}))
pg_cursor.execute("SELECT * FROM app_student")
pg_data = pg_cursor.fetchall()
combined_data = []
for mongo_record in mongo_data:
    for pg_record in pg_data:
        if mongo_record["studentId"] == pg_record[0]:  # ปรับให้ตรงกับคีย์ของคุณ
            combined_record = {**mongo_record, **pg_record}
            combined_data.append(combined_record)
import csv

csv_columns = list(combined_data[0].keys())  # กำหนดคอลัมน์ตามคีย์ของข้อมูลที่รวมกันแล้ว

with open("combined_data.csv", "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
    writer.writeheader()
    for data in combined_data:
        writer.writerow(data)

print("Data has been written to combined_data.csv")
