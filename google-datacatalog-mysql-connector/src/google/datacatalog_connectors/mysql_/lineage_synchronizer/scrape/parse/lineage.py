from collections import OrderedDict
from functools import reduce
from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape.parse.operation import getColumnInfo, handleSource
from iteration_utilities import unique_everseen 

def handleInsert(node):
    # extract target data
    target = {"tables": [getColumnInfo(node.targetTable)], "columns": ['*']}

    if node.columnList is not None:
        target['columns'] = reduce(lambda x, y: x + getColumnInfo(y),
                                   node.columnList, [])

    # exctract source data
    source = handleSource(node)

    return {"source": source, "target": target}


def handleSelect(node):
    return {
        "source": handleSource(node)
    }


def handleQuery(node):
    return extractLineage(node.query)


def handleCreateTable(node):
    res = {
        "target": {
            "tables": [],
            "columns": []
        },
        "source": handleSource(node.query)
    }

    res['target']['tables'] = [getColumnInfo(node.name)]

    if hasattr(node, 'columnList') and node.columnList is not None:
        res['target']['columns'] = reduce(lambda x, y: x + [getColumnInfo(y)],
                                          node.columnList, [])
    else:
        res['target']['columns'] = ['*']

    return res


def extractLineage(node):
    if hasattr(node, 'operator'):
        if node.operator.kind == "CREATE_TABLE":
            return handleCreateTable(node)

    if hasattr(node, 'targetTable'):
        return handleInsert(node)

    if hasattr(node, 'selectList'):
        return handleSelect(node)

    if hasattr(node, 'query'):
        return handleQuery(node)

    return False


def removeDuplicates(myList: list):
    return list(unique_everseen(myList))

def combineLineageEvents(*lineageEvents: dict):
    combinedEvents = {
        'target' :[], 
        'source': []
    }
    
    for event in lineageEvents:
        if 'target' in event:
            target =  event['target'] 
            if not isinstance(target,list):
                target = [event['target']]
                
            combinedEvents['target'] += target

        if 'source' in event:
            source =  event['source'] 
            if not isinstance(source,list):
                source = [event['source']]
            combinedEvents['source'] += source
    
    # ensure that source are unique 
    combinedEvents['target'] = removeDuplicates(combinedEvents['target'])
    combinedEvents['source'] = removeDuplicates(combinedEvents['source'])
    
    return combinedEvents

def extractLineageFromList(nodelist):
    lineageTreeList = []

    for nodeTree in nodelist:
        lineageTreeList.append(extractLineage(nodeTree))
        
    return combineLineageEvents(*lineageTreeList)
         