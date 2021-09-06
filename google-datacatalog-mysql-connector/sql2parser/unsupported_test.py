import pathlib
import unittest
import parseSql
import json


def getQueryAndExpected(filename):
    # get relative path
    scriptDir = pathlib.Path(__file__).parent.resolve()

    # open files
    f1 = open(str(scriptDir) + "/tests/" + filename + '/query.sql', "r")
    f2 = open(str(scriptDir) + "/tests/" + filename + '/parsed.json', "r")

    # read files
    query, parsed = f1.read(), f2.read()

    # close files
    f1.close()
    f2.close()

    return query, parsed


def testSqlfile(test, filename):
    query, expectedResult = getQueryAndExpected(filename)
    expectedResult = json.loads(expectedResult)
    result = parseSql.parseQuery(query)
    print("")
    print("Result: ", result)
    print("Expected: ", expectedResult)

    test.assertEqual(result, expectedResult)


class UnsupporterdTestParser(unittest.TestCase):
    def test_create_table_with_values_as_select(self):
        testname = "CreateTableWithValuesAsSelect"
        testSqlfile(self, testname)

    def test_simple_insert_values(self):
        testname = "simpleInsertValues"
        testSqlfile(self, testname)

    def test_simple_update_table(self):
        testname = "simpleUpdateTable"
        testSqlfile(self, testname)


if __name__ == '__main__':
    unittest.main()
