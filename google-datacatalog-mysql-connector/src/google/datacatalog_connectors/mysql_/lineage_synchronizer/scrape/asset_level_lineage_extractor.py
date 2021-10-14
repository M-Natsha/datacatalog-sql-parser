import re
from typing import Sequence
from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape.parse.transform_equ.commons import Commons

from .parse import parseSql
from .parse.lineage import remove_duplicates
from .parse.operation import append_or_extend

noLineageQueryPatterns = [
    re.compile("\\s*SET\\s+((?s).*)", re.IGNORECASE),
    re.compile("\\s*DROP\\s+((?s).*)", re.IGNORECASE),
    re.compile("\\s*EXPLAIN\\s+((?s).*)", re.IGNORECASE),
    re.compile("\\s*SHOW\\s+((?s).*)", re.IGNORECASE),
    re.compile("\\s*DESC\\s+((?s).*)", re.IGNORECASE),
    re.compile("\\s*DESCRIBE\\s+((?s).*)", re.IGNORECASE),
    re.compile("\\s*DESCRIBE\\s+((?s).*)", re.IGNORECASE),
    re.compile("\\s*use\\s+((?s).*)", re.IGNORECASE),
    re.compile("\\s*select\\s+((?s).*)", re.IGNORECASE),
    re.compile("\\s*CREATE\\s+TABLE((?s).*)\\s+ENGINE\\s+=\\s+[A-Za-z1-9]+;",
               re.IGNORECASE),
]

class AssetLevelLineageExtractor:
    @staticmethod
    def _remove_aliases(myList: Sequence) -> Sequence[str]:
        """ Remove alias names of the tables

        Args:
            myList: List of table names (string) and aliase object

        Returns:
            filtered_list: list of table names removing all aliases
        """
        finalResult = myList
        aliases = set()

        for table in myList:
            if 'operation' in table and table['operation'] == "AS":
                append_or_extend(finalResult, table['input'])
                aliases.add(table['output'])
        filtered_list = [
            table for table in finalResult
            if isinstance(table, str) and table not in aliases
        ]

        return filtered_list

    @staticmethod
    def _is_insert_values(text: str) -> bool:
        """Checks if a Sql query is "Insert <table_name> Values <Value-List>" query

        Args:
            text: Sql query string
        """
        insertIntoKeyword = Commons.findSqlKeyword(text, "INSERT\\s+INTO")
        valuesKeyword = Commons.findSqlKeyword(text, "VALUES")

        return insertIntoKeyword != -1 and valuesKeyword != -1

    def query_has_lineage(self, query: str):
        """Quickly check if a query has lineage information
        It might result in some false positive
        """
        
        global noLineageQueryPatterns

        for pattern in noLineageQueryPatterns:
            if pattern.match(query):
                return False

        if (AssetLevelLineageExtractor._is_insert_values(query)):
            return False

        return True  # else it has lineage info

    def extract_from_node_helper(self, node):
        if isinstance(node, str):
            if node == "_":
                return
            return node

        if isinstance(node, list):
            tableArray = []
            for source in node:
                source = self.extract_from_node_helper(source)
                if isinstance(source, list):
                    tableArray += source
                elif source is not None:
                    tableArray += [source]

            return remove_duplicates(tableArray)

        if 'target' in node and 'source' in node:
            return {
                'target': self.extract_from_node_helper(node['target']),
                'source': self.extract_from_node_helper(node['source'])
            }

        if 'tables' in node:
            result = self.extract_from_node_helper(node['tables'])
            return AssetLevelLineageExtractor._remove_aliases(result)

        if 'operation' in node:
            if (node['operation'] == "AS"):
                node['input'] = self.extract_from_node_helper(node['input'])
                return node
            elif node['operation'] == "GET_COLUMN":
                return self.extract_from_node_helper(node['input'])
            else:
                return self.extract_from_node_helper(node['input'])

    def extract_from_node(self, node):
        table_lineage = self.extract_from_node_helper(node)
        return table_lineage

    def _get_sql_parser(self):
        return parseSql.MySqlParser

    def extract(self, query:str):
        """Parses an SQL query and then extract lineage info from it

        Args:
            query (str): A SQL query

        Returns:
            Lineage parse tree
        """
        parser = self._get_sql_parser()() 
        parseTree = parser.parse_query(query) # Parse sql query
        lineage = self.extract_from_node(parseTree)  # extract lineage tree
        return lineage
