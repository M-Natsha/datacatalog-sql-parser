import logging
from google.datacatalog_connectors.mysql_.scrape import metadata_scraper
from google.datacatalog_connectors.mysql_.lineage_extractor \
    import logs_reader, asset_level_lineage_extractor


class AssetLevelLinneagScraper():

    def __init__(self, connection_args):
        self.connection_args = connection_args

    def scrape(self):
        # get connection
        connector = self._get_connector()()
        connection = connector.get_connection(self.connection_args)

        # read logs
        reader = self._get_log_reader()(connection)
        logs = reader.read_logs()

        # extract lineage
        lineageList = []
        lineage_extractor = self._get_lineage_extractor()()

        for log in logs:
            if log['command_type'] == 'Query':
                query = log['argument'].decode('ascii')
                if lineage_extractor.query_has_lineage(query):
                    try:
                        lineage = lineage_extractor.extract(query)
                        print(lineage)
                        lineageList.extend(lineage)
                    except Exception as e:
                        logging.error("Parse error: Couldn't parse " + query)
                        print(e)

                else:
                    logging.info("Query has no lineage (Skipped): " + query)

        # return lineage info
        return lineageList

    def _get_connector(self):
        return metadata_scraper.MetadataScraper

    def _get_log_reader(self):
        return logs_reader.LogsReader

    def _get_lineage_extractor(self):
        return asset_level_lineage_extractor.AssetLevelLineageExtractor
