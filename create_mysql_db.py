#from __future__ import print_function
import mysql.connector
from mysql.connector import errorcode


DB_NAME = 'anomalies_db'

TABLES = {}
TABLES[DB_NAME] = (
    "CREATE TABLE anomalies ("
    "  `timeframe_start` datetime NOT NULL,"
    "  `api_name` varchar(40) NOT NULL,"
    "  `http_method` varchar(6) NOT NULL,"
    "  `count_http_code_5xx` mediumint,"
    "  `is_anomaly` varchar(3),"
    "  UNIQUE KEY `ix_time` (`timeframe_start`, `api_name`, `http_method`)"
    ") ENGINE=InnoDB")


def create_database(cursor, cnx):
    try:
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))


    try:
        cnx.database = DB_NAME
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor, cnx)
            cnx.database = DB_NAME
        else:
            print(err)


    for name, ddl in iter(TABLES.items()):

        try:
            print("Creating table {}: ".format(name), end='')
            cursor.execute(ddl)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

    cursor.close()

if __name__ == '__main__':
    cnx = mysql.connector.connect(user = 'root', password = 'hungreed', host = '127.0.0.1')
    cursor = cnx.cursor()
    create_database(cursor, cnx)
    cnx.close()