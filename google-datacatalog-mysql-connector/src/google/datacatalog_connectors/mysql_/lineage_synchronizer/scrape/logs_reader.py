class logsReader():

    def __init__(self, sqlConnection):
        self._sqlConnection = sqlConnection

    def read_logs(self):
        """Returns MySQL logs 
        Note: table logs must be activated
        """
        mycursor = self._sqlConnection.cursor(dictionary=True)
        mycursor.execute("select * from mysql.general_log")

        return mycursor.fetchall()
