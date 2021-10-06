from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape import asset_level_lineage_extractor
from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape.parse import parseSql
from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape.parse import parseSql
from .utils import *


class TestHasLineageChecker():

    def test_simple_insert_values(self, helpers):
        testname = "simpleInsertValues"
        query = helpers.getQueryForTest(testname)
        expected = False
        result = asset_level_lineage_extractor.AssetLevelLineageExtractor(
        ).query_has_lineage(query)
        assert result == expected
