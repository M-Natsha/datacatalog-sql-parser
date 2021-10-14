import jpype
import jpype.imports
from importlib import import_module
import jars


class JavaParserConnector:
    """Wrapper for apache calcite parser"""
    _parse_sql = None

    def __init__(self):
        if jpype.isJVMStarted():
            return

        path2jar = jars.__file__
        path2jar = path2jar.replace('__init__.py', 'sql-parser.jar')
        jpype.startJVM(classpath=[path2jar])
        JavaParserConnector._parse_sql = import_module('com.gsql.ParseSql')

    def parse_query(self, query: str):
        """Parses an Sql query using apache calcite

        Args:
            query (str): a Sql query

        Returns:
            Apache parsed tree
        """
        return JavaParserConnector._parse_sql.parseSqlToJson(query)
