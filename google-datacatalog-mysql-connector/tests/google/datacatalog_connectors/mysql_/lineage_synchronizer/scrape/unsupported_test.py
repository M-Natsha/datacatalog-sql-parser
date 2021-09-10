from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape.parse import parseSql
from .utils import *

class TestSqlParser(): 
    @pytest.mark.skip(reason="no way of currently testing this")
    def test_create_table_with_values_as_select(self, helpers):
        testname = "CreateTableWithValuesAsSelect"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getParsedResultForTest(testname)
        result = parseSql.MySqlParser().parseQuery(query)
        assert result == expected
        
    @pytest.mark.skip(reason="no way of currently testing this")
    def test_simple_insert_values(self, helpers):
        testname = "simpleInsertValues"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getParsedResultForTest(testname)
        result = parseSql.MySqlParser().parseQuery(query)
        assert result == expected
        

    @pytest.mark.skip(reason="no way of currently testing this")
    def test_simple_update_table(self, helpers):
        testname = "simpleUpdateTable"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getParsedResultForTest(testname)
        result = parseSql.MySqlParser().parseQuery(query)
        assert result == expected    