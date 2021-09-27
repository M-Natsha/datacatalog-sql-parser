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

    def test_create_table_with_values_as_select(self, helpers):
        testname = "CreateTableWithValuesAsSelect"
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

    def test_delete_and_select(self, helpers):
        testname = "DeleteAndSelect"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected

    def test_delete_and_select2(self, helpers):
        testname = "DeleteAndSelect2"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected

    def test_delete_from_join(self, helpers):
        testname = "DeleteFromJoin"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected

    def test_delete_from_multiple_tables(self, helpers):
        testname = "DeleteFromMultipleTable"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected

    def test_select_where_compare(self, helpers):
        testname = "selectWhereCompare"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected

    def test_select_where_select(self, helpers):
        testname = "selectWhereSelect"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected

    def test_simple_insert_from(self, helpers):
        testname = "simpleInsertFrom"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected

    def test_simple_select(self, helpers):
        testname = "simpleSelect"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected

    def test_simple_update_table(self, helpers):
        testname = "simpleUpdateTable"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected

    def test_update(self, helpers):
        testname = "update"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected

    def test_update2(self, helpers):
        testname = "update2"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected

    def test_update_multiple_tables(self, helpers):
        testname = "updateMultipleTables"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected

    def test_update_set_select(self, helpers):
        testname = "updateSetSelect"
        query = helpers.getQueryForTest(testname)
        expected = helpers.getTableLineageResultForTest(testname)
        result = tableLineageExtractor().extract(query)
        print(result)
        assert result == expected
