import pathlib

from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape.table_lineage_extractor import tableLineageExtractor
import json
import pytest

scriptDir = pathlib.Path(__file__).parent.resolve()

test_data_dir =  str(scriptDir) + "/test_data/";

class Helpers:
    @staticmethod    
    def getQueryForTest(filename):
        global test_data_dir
        f1 = open(test_data_dir + filename + '/query.sql', "r")
    
        # read files
        query = f1.read()
        # close files
        f1.close()

        return query

    @staticmethod   
    def getTableLineageResultForTest(filename):
        global test_data_dir
        f1 = open(test_data_dir + filename + '/tableLineage.json', "r")
    
        # read files
        query = f1.read()
        # close files
        f1.close()

        return json.loads(query)

    @staticmethod    
    def getParsedResultForTest(filename):
        global test_data_dir
        f1 = open(test_data_dir + filename + '/parsed.json', "r")
    
        # read files
        query = f1.read()
        # close files
        f1.close()

        return json.loads(query)

    @staticmethod
    def testEquality(test, result, expectedResult):
        print("")
        print("Result: ", result)
        print("Expected: ", expectedResult)

        test.assertEqual(result, expectedResult)
        
        
@pytest.fixture
def helpers():
    return Helpers
