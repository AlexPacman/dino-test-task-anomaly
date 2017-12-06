import csv
import re
from datetime import datetime, timedelta
import mysql.connector
import create_mysql_db


# regulars and constants
pattern = r"5.."
fifteen_minutes = timedelta(minutes=15)
d = {}
# query
query_add_data = (
            "INSERT INTO anomalies "
            "(timeframe_start, api_name, http_method, count_http_code_5xx) "
            "VALUES (%s, %s, %s, %s)")


def fill_dict(d, file_reader):

    for row in file_reader:
        row[0] = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S,%f')
        if d.get((row[1], row[2])) is None:
            d.update({(row[1], row[2]): [0, row[0]]})
        elif row[0] - d[(row[1], row[2])][-1] > fifteen_minutes:
            d[(row[1], row[2])].append(0)
            d[(row[1], row[2])].append(row[0])
        if re.match(pattern, row[3]):
            d[(row[1], row[2])][-2] += 1


def fill_tablet(d, f_cursor):

    for keys, values in iter(d.items()):
            for i in range(0, len(values), 2):
                data_anomaly = (values[i + 1], keys[0], keys[1], values[i])
                f_cursor.execute(query_add_data, data_anomaly)


# connecting to MySQL server
print("To create and connect to MySQLdb enter the data in one line."
      "\nuser_name, password, host_ip.")
user, password, host = input().split()
cnx = mysql.connector.connect(user=user, password=password, host=host)
cursor = cnx.cursor()
create_mysql_db.create_database(cursor, cnx)
cnx.close()

# connecting to database
cnx = mysql.connector.connect(user=user, password=password, host=host, database=create_mysql_db.DB_NAME)
cursor = cnx.cursor()
print("Connected successfully. Script is working")

input_file = open('raw_data.csv', 'r')
rdr = csv.reader(input_file)
next(rdr)

# read the file line by line, group data in dict and count number of 5xx errors
fill_dict(d, rdr)

# fill the tablet
fill_tablet(d, cursor)

cnx.commit()
cursor.close()
cnx.close()
input_file.close()
print("Done!")