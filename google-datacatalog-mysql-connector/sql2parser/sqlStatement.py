from functools import reduce
from operation import *


def getColumnInfo(node):
    if hasattr(node, 'operator'):
        return operatorFuncMap[node.operator.kind](node)

    if node.names[0] == '':
        return '*'

    return node.names[0]


def HandleUnion(node):
    par = {
        "operation": "UNION",
        "input": []
    }

    for query in node.operands:
        par["input"] += [handleSource(query)]
    return par


def handleSource(node):
    if(hasattr(node, 'source')):
        return handleSource(node.source)

    # return names
    if(hasattr(node, 'names')):
        return node.names

    # handle from
    if(hasattr(node, 'from')):
        return handleFrom(node)

    # handle Union
    if (hasattr(node, 'operator') and node.operator.kind == "UNION"):
        return HandleUnion(node)

    if(hasattr(node, 'left')):
        return handleJoin(node)

    return []


def handleJoin(node):

    par = {
        "operation": "JOIN",
        "input": []
    }

    if(hasattr(node, 'left')):
        par['input'] += handleSource(node.left)

    if(hasattr(node, 'right')):
        par['input'] += handleSource(node.right)

    return par


def handleFrom(node):
    frm = getattr(node, 'from')

    source = {
        "tables": handleSource(frm),
        "columns": reduce(lambda x, y: x + [getColumnInfo(y)], node.selectList, [])
    }

    return source
