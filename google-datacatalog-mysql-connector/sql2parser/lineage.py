import json
from types import SimpleNamespace
from functools import reduce
from sqlStatement import *

def handleInsert(node):   
    # extract target data 
    target = {
        "table" : node.targetTable.names[0],
        "columns" : []
    } 
    
    if(node.columnList != None):
        target['columns']= reduce(lambda x,y:x + [getColumnInfo(y)] , node.columnList, [])
        
    # exctract source data  
    source = handleSource(node)
                           
    return { "source": source, "target": target}

def handleSelect(node):
    return handleSource(node)

def handleQuery(node):
    return extractLineage(node.query);

def extractLineage(node):
    if(hasattr(node,'targetTable')):
        return handleInsert(node)
    
    if(hasattr(node,'selectList')):
        return handleSelect(node)
    
    if(hasattr(node,'query')):
        return handleQuery(node)
    
    return False
