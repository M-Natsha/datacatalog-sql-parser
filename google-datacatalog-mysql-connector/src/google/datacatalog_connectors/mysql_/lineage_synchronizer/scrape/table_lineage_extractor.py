import re

from .parse import parseSql
from .parse.java_parser_connector import JavaParserConnector
from .parse.lineage import remove_duplicates
from .parse.operation import AppendOrExtend

noLineageQueryPatterns = [
    re.compile("\\s*SET\\s+.*", re.IGNORECASE),
    re.compile("\\s*DROP\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*EXPLAIN\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*SHOW\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*DESC\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*DESCRIBE\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*DESCRIBE\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*use\\s+(.*)", re.IGNORECASE),
    re.compile("\s*CREATE\s+TABLE((?s).*)\s+ENGINE\s+=\s+[A-Za-z1-9]+;",
               re.IGNORECASE),
]


def is_insert_values(text):
    javaParserConnector = JavaParserConnector()
    insertIntoKeyword = javaParserConnector.get_sql_keyword(
        text, "\\s*INSERT\\s+INTO")
    valuesKeyword = javaParserConnector.get_sql_keyword(text, "VALUES")

    print("keywords is ", insertIntoKeyword, valuesKeyword)
    return insertIntoKeyword != -1 and valuesKeyword != -1


def remove_aliases(myList):
    finalResult = myList
    aliases = set()

    for table in myList:
        if 'operation' in table and table['operation'] == "AS":
            AppendOrExtend(finalResult, table['input'])
            aliases.add(table['output'])

    return [
        table for table in finalResult
        if isinstance(table, str) and table not in aliases
    ]


class tableLineageExtractor:

    def query_has_lineage(self, query):
        global noLineageQueryPatterns

        for pattern in noLineageQueryPatterns:
            if pattern.match(query):
                return False

        if (is_insert_values(query)):
            return False

        # else it has lineage info
        return True

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
            return remove_aliases(result)

        if 'operation' in node:
            if (node['operation'] == "AS"):
                node['input'] = self.extract_from_node_helper(node['input'])
                print(node)
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

    def extract(self, query):
        # Parse sql query
        parser = self._get_sql_parser()()
        parseTree = parser.parse_query(query)

        # extract lineage
        lineage = self.extract_from_node(parseTree)
        # return lineage
        return lineage
