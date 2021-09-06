import json
from types import SimpleNamespace
from functools import reduce
from operation import *


def handleInsert(node):
    # extract target data
    target = {
        "table": getColumnInfo(node.targetTable),
        "columns": ['*']
    }

    if(node.columnList != None):
        target['columns'] = reduce(
            lambda x, y: x + getColumnInfo(y), node.columnList, [])

    # exctract source data
    source = handleSource(node)

    return {"source": source, "target": target}


def handleSelect(node):
    return handleSource(node)


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

    res['target']['tables'] = getColumnInfo(node.name)

    if(hasattr(node, 'columnList') and node.columnList != None):
        res['target']['columns'] = reduce(
            lambda x, y: x + [getColumnInfo(y)], node.columnList, [])
    else:
        res['target']['columns'] = ['*']

    return res


def extractLineage(node):
    if(hasattr(node, 'operator')):
        if(node.operator.kind == "CREATE_TABLE"):
            return handleCreateTable(node)

    if(hasattr(node, 'targetTable')):
        return handleInsert(node)

    if(hasattr(node, 'selectList')):
        return handleSelect(node)

    if(hasattr(node, 'query')):
        return handleQuery(node)

    return False
