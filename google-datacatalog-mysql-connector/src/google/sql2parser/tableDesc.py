# how to organize the database for the schemas
# create columns with cursorstamp range in which this table is available
# and create a log history to show all


def isExist(action):
    # TODO: Write code that check if exists depending on the action
    return True


class TableSchema:
    schema = {}
    logs = []

    def consturctor(tableName):
        # make it create a file if there is no file for this table
        # else read the cache of that table
        pass

    def addInfo(self, info, cursorstamp):
        # add info at a specific cursor stamp
        self.logs += [info]

        for col in info.action.columns:
            state = {"cursorstamp": cursorstamp, "exists": isExist(info)}

            if col in self.schema:
                self.schema[col] += [state]
            else:
                self.schema = [state]

            # TODO sort self.schemya
        pass

    def getSchema(self, cursorstamp):
        # returns a specific schema at a specific time
        schema = {}

        for col in self.schema:
            # write it as a function and use cache maybe?
            # binary search
            # find nearest change
            # decide if this column exist
            pass
        return schema

    def save():
        # save changes for this table into a file (cache)
        pass
