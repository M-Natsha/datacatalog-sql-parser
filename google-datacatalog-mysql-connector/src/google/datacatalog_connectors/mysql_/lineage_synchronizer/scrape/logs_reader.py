class logsReader():
    def __init__(self, sqlConnection):
        self._sqlConnection = sqlConnection

    def readLogs(self):
        mycursor = self._sqlConnection .cursor(dictionary=True)
        mycursor.execute("select * from mysql.general_log")

        return mycursor.fetchall()
