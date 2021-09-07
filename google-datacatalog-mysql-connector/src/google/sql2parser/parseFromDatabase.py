from datetime import timezone
from parseSql import parseQuery, hasLineage
from tableLineage import getTableLineage

# TODO: use import instead of copying the class


def _create_rdbms_connection(connection_args):
    # import at the method level, because this flow is conditional
    # if the connector reads from a CSV file, this is not used.
    from mysql.connector import connect  # noqa C6204

    con = connect(database=connection_args['database'],
                  host=connection_args['host'],
                  user=connection_args['user'],
                  password=connection_args['pass'])
    return con


def readLogs(con):
    mycursor = con.cursor(dictionary=True)
    mycursor.execute("select * from mysql.general_log")

    return mycursor.fetchall()


con = _create_rdbms_connection({
    "database": "dummy_client",
    "host": "localhost",
    "user": "root",
    "pass": "Test1234"
})


logs = readLogs(con)

for log in logs:
    if(log["command_type"].lower() == "query"):
        # getTimeStamp
        datetime = log["event_time"]
        timestamp = datetime.replace(tzinfo=timezone.utc).timestamp()

        query = log["argument"].decode('ascii')
        print(timestamp, query)
        if(hasLineage(query)):
            try:
                lineage = getTableLineage(query)
                print(lineage)
            except Exception:
                print("ERR")
        else:
            print("SKIPPED")

        print('--------------------------')
