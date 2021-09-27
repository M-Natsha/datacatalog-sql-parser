import jpype
import jpype.imports
from importlib import import_module
from os import path


class JavaParserConnector:
    _ParseSql = None
    _transaform_commons = None

    def __init__(self):
        if jpype.isJVMStarted():
            return

        # TODO: use relative path and/or move the path to enviroment
        dir_path = path.dirname(path.realpath(__file__))
        path2jar = dir_path + \
            "/extractLineage/target/sql-parser-0.1-jar-with-dependencies.jar"
        jpype.startJVM(classpath=[path2jar])
        JavaParserConnector.ParseSql = import_module('com.ParseSql')
        JavaParserConnector._transaform_commons = import_module(
            'com.sql.transform.Commons')

    def parse_query(self, query):
        return JavaParserConnector.ParseSql.parseSqlToJson(query)

    def get_sql_keyword(self, text, keyword):
        return JavaParserConnector._transaform_commons.findSqlKeyword(
            text, keyword)
