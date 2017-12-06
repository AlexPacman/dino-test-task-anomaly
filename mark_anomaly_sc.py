import mysql.connector
import numpy as np
from create_mysql_db import DB_NAME


# variables
my_list = []

# queries
query_select_5xx_lines = (
        "select timeframe_start, api_name, http_method,"
        " count_http_code_5xx from anomalies_db.anomalies"
        " where count_http_code_5xx > 0")
query_select_5xx_by_api_http = (
        "select count_http_code_5xx from anomalies_db.anomalies"
        " where api_name = %s and http_method = %s")
query_update_anomaly_sigma = (
        "update anomalies set"
        " is_anomaly = if(count_http_code_5xx >= %s, 'yes', 'no')"
        " where timeframe_start = %s and api_name = %s and http_method = %s;")
query_update_anomaly_zeros = (
        "update anomalies set"
        " is_anomaly = 'no'"
        " where count_http_code_5xx = 0;")


# calculate sigma for each pair api_name, http_method
def count_sigma(f_cursor):
    for count_http_code_errors in f_cursor:
        my_list.append(count_http_code_errors[0])

    c = np.asarray(my_list)
    my_list.clear()
    sigma = float(np.std(c))
    return sigma


def mark_anomaly(f_cursor):
    for (timeframe_start, api_name, http_method, count_http_code_5xx) in f_cursor:

        cursor2 = cnx.cursor()
        cursor2.execute(query_select_5xx_by_api_http, (api_name, http_method))
        sigma = count_sigma(cursor2)
        cursor2.close()

        cursor_mark_the_anomaly = cnx.cursor()
        cursor_mark_the_anomaly.execute(query_update_anomaly_sigma, (sigma * 3, timeframe_start, api_name, http_method))
        cursor_mark_the_anomaly.close()


# mark as 'no anomaly' lines where count_http_code_5xx == 0
def update_zeros_anomaly():
    cursor2 = cnx.cursor()
    cursor2.execute(query_update_anomaly_zeros)
    cursor2.close()


print("To connect to MySQLdb enter the data in one line."
      "\nuser_name, password, host_ip.")
user, password, host = input().split()
cnx = mysql.connector.connect(user=user, password=password, host=host, database=DB_NAME)
cursor = cnx.cursor(buffered=True)
cursor.execute(query_select_5xx_lines)
mark_anomaly(cursor)
cursor.close()

update_zeros_anomaly()

cnx.commit()
cnx.close()
print("Done!")
