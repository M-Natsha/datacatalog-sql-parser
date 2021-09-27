import jpype
import jpype.imports
from jpype.types import *
from importlib import import_module

class JavaParserConnector:    
    _ParseSql = None
    _transaform_commons = None
    
    def __init__(self):
        if jpype.isJVMStarted():
            return
        
        # TODO: use relative path and/or move the path to enviroment
        path2jar = "/home/natsha/work/datacatalog-connectors-rdbms/google-datacatalog-mysql-connector/src/google/datacatalog_connectors/mysql_/lineage_synchronizer/scrape/parse/extractLineage/target/test-1.0-SNAPSHOT-jar-with-dependencies.jar"        
        jpype.startJVM(classpath=[path2jar])
        JavaParserConnector.ParseSql = import_module('com.ParseSql')
        JavaParserConnector._transaform_commons = import_module('com.sql.transform.Commons')
        
        
    def parse_query(self, query):
        return JavaParserConnector.ParseSql.parseSqlToJson(query)
    
    
    def get_sql_keyword(self, text, keyword):
        return JavaParserConnector._transaform_commons.findSqlKeyword(text,keyword)
    