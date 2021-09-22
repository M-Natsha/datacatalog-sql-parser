from collections import OrderedDict
from functools import reduce
from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape.parse.operation import getColumnInfo, handleSource
from iteration_utilities import unique_everseen 

def handle_insert(node):
    # extract target data
    target = {"tables": [getColumnInfo(node.targetTable)], "columns": ['*']}

    if node.columnList is not None:
        target['columns'] = reduce(lambda x, y: x + getColumnInfo(y),
                                   node.columnList, [])

    # exctract source data
    source = handleSource(node)

    return {"source": source, "target": target}


def handle_select(node):
    return {
        "source": handleSource(node)
    }


def handle_query(node):
    return extract_lineage(node.query)


def handle_create_table(node):
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

    
def extract_lineage(node):
    if hasattr(node, 'operator'):
        if node.operator.kind == "CREATE_TABLE":
            return handle_create_table(node)

    if hasattr(node, 'targetTable'):
        return handle_insert(node)

    if hasattr(node, 'selectList'):
        return handle_select(node)

    if hasattr(node, 'query'):
        return handle_query(node)

    return False


def remove_duplicates(myList: list):
    return list(unique_everseen(myList))

def combine_lineage_events(*lineageEvents: dict):
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
    combinedEvents['target'] = remove_duplicates(combinedEvents['target'])
    combinedEvents['source'] = remove_duplicates(combinedEvents['source'])
    
    return combinedEvents

def extract_lineage_from_list(nodelist):
    lineageTreeList = []

    for nodeTree in nodelist:
        lineageTreeList.append(extract_lineage(nodeTree))
        
    return combine_lineage_events(*lineageTreeList)
         