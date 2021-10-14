import logging
from google.datacatalog_connectors.mysql_.lineage_extractor \
    import logs_reader, asset_level_lineage_extractor
from google.datacatalog_connectors.mysql_.parse.parse_sql import MySqlParser


class AssetLevelLinneagScraper():

    def __init__(self, connection):
        self.connection = connection

    def scrape(self):
        # read logs
        reader = self._get_log_reader()(self.connection)
        logs = reader.read_logs()

        # extract lineage
        lineageList = []
        lineage_extractor = self._get_lineage_extractor()()

        for log in logs:
            if log['command_type'] == 'Query':
                query = log['argument'].decode('ascii')
                if lineage_extractor.query_has_lineage(query):
                    try:
                        log = f'Parsing Query: {query}\n'
                        parse_tree = self._get_sql_parser()().parse_query(
                            query)
                        log += '---------------- Parse Tree ----------------\n'
                        log += str(parse_tree) + '\n'
                        log += '----------------- lineage  -----------------\n'
                        lineage = lineage_extractor \
                            .extract_asset_lineage_from_parsed_tree(parse_tree)
                        log += str(lineage) + '\n'
                        lineageList.extend(lineage)
                        logging.info(log)
                    except Exception as e:
                        logging.error("Parse error: Couldn't parse: " + query)
                        print(e)

                else:
                    logging.info("Query has no lineage (Skipped): " + query)

        # return lineage info
        return lineageList

    def _get_log_reader(self):
        return logs_reader.LogsReader

    def _get_sql_parser(self):
        return MySqlParser

    def _get_lineage_extractor(self):
        return asset_level_lineage_extractor.AssetLevelLineageExtractor
