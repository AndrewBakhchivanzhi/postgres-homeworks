import psycopg2
import csv


def castomers_data_csv():
    items = []
    with open("C:\\Users\\Andrew\\PycharmProjects\\postgres-homeworks\\homework-1\\north_data\\customers_data.csv",
              newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            item = (row['customer_id'], row['company_name'], row['contact_name'])
            items.append(item)
    return items

def employees_data_csv():
    items = []
    with open("C:\\Users\\Andrew\\PycharmProjects\\postgres-homeworks\\homework-1\\north_data\\employees_data.csv",
              newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            item = (int(row['employee_id']), row['first_name'], row['last_name'], row['title'], row['birth_date'], row['notes'])
            items.append(item)
    return items

def orders_data_csv():
    items = []
    with open("C:\\Users\\Andrew\\PycharmProjects\\postgres-homeworks\\homework-1\\north_data\\orders_data.csv",
              newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            item = (int(row['order_id']), row['customer_id'], int(row['employee_id']), row['order_date'], row['ship_city'])
            items.append(item)
    return items

with psycopg2.connect(host='localhost', database='north', user='postgres', password='krot123.A') as conn:
    with conn.cursor() as cur:
        cur.executemany("INSERT INTO customers VALUES (%s, %s, %s)", castomers_data_csv())

    with conn.cursor() as cur:
        cur.executemany("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", employees_data_csv())

    with conn.cursor() as cur:
        cur.executemany("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", orders_data_csv())

conn.close()









