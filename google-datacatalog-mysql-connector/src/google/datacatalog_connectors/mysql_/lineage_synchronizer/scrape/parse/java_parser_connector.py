import jpype
import jpype.imports
from importlib import import_module
from os import path


class JavaParserConnector:
    """Wrapper for apache calcite parser"""
    _parse_sql = None

    def __init__(self):
        if jpype.isJVMStarted():
            return

        # TODO: use relative path and/or move the path to enviroment
        dir_path = path.dirname(path.realpath(__file__))
        path2jar = dir_path + \
            "/extractLineage/target/sql-parser-0.1-jar-with-dependencies.jar"
        
        path2jar = "/home/natsha/work/dc/google-datacatalog-mysql-connector/calcite-sql-parser/target/sql-parser-0.1-jar-with-dependencies.jar"        
        
        jpype.startJVM(classpath=[path2jar])
        JavaParserConnector._parse_sql = import_module('com.ParseSql')


    def parse_query(self, query: str):
        """Parses an Sql query using apache calcite

        Args:
            query (str): a Sql query

        Returns:
            Apache parsed tree
        """
        return JavaParserConnector._parse_sql.parseSqlToJson(query)