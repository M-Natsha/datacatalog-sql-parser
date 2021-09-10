from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape.table_lineage_extractor import tableLineageExtractor
from .utils import *

class TestTableLineage():
    def test_simple_select(self, helpers):
        testname = "simpleSelect"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected

    def test_complex_select_with_join(self, helpers):
        testname = "complexSelectWithJoin"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected

    def test_complex_select_with_union_and_join(self, helpers):
        testname = "complexSelectWithUnionAndJoin"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected

    def test_create_table_as_select(self, helpers):
        testname = "CreateTableAsSelect"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected

    def test_create_table_with_values_as_select2(self, helpers):
        testname = "CreateTableWithValuesAsSelect2"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected

    def test_simple_insert(self, helpers):
        testname = "simpleInsertFrom"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected
        