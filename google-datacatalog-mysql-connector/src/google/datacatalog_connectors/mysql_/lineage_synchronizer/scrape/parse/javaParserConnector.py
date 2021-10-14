import jpype
import jpype.imports
from jpype.types import *
from importlib import import_module

class javaParserConnector:    
    _ParseSql = None
    
    def __init__(self):
        if jpype.isJVMStarted():
            return
        
        # TODO: use relative path and/or move the path to enviroment
        path2jar = "/home/natsha/work/datacatalog-connectors-rdbms/google-datacatalog-mysql-connector/src/google/datacatalog_connectors/mysql_/lineage_synchronizer/scrape/parse/extractLineage/target/test-1.0-SNAPSHOT-jar-with-dependencies.jar"        
        jpype.startJVM(classpath=[path2jar])
        javaParserConnector.ParseSql = import_module('com.ParseSql')
        
    def parseQuery(self, query):
        return javaParserConnector.ParseSql.parseSqlToJson(query)