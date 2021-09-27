from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape import table_lineage_extractor
from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape.parse import parseSql
from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape.parse import parseSql
from .utils import *

class TestHasLineageChecker():
    def test_simple_insert_values(self, helpers):
        testname = "simpleInsertValues"
        query = helpers.getQueryForTest(testname)
        expected = False
        result = table_lineage_extractor.tableLineageExtractor().query_has_lineage(query)
        assert result == expected