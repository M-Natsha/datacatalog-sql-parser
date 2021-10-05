import jpype
import jpype.imports
from importlib import import_module
from os import path


class JavaParserConnector:
    _ParseSql = None

    def __init__(self):
        if jpype.isJVMStarted():
            return

        # TODO: use relative path and/or move the path to enviroment
        dir_path = path.dirname(path.realpath(__file__))
        path2jar = dir_path + \
            "/extractLineage/target/sql-parser-0.1-jar-with-dependencies.jar"
        
        jpype.startJVM(classpath=[path2jar])
        JavaParserConnector._ParseSql = import_module('com.ParseSql')


    def parse_query(self, query):
        return JavaParserConnector._ParseSql.parseSqlToJson(query)