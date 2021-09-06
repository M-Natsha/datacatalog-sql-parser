from functools import reduce


def handleJoin(node):
    par = {
        "operation": "JOIN",
        "input": []
    }

    if(hasattr(node, 'left')):
        par['input'] += [handleSource(node.left)]

    if(hasattr(node, 'right')):
        par['input'] += [handleSource(node.right)]

    return par


def handleFrom(node):
    frm = getattr(node, 'from')

    source = {
        "tables": handleSource(frm),
        "columns": reduce(lambda x, y: x + [getColumnInfo(y)], node.selectList, [])
    }

    return source


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
    if (hasattr(node, 'operator')):
        return handleOperation(node)

    if(hasattr(node, 'left')):
        return handleJoin(node)

    if(hasattr(node, 'query')):
        return handleSource(node.query)['tables']

    return []


def HandleUnion(node):
    par = {
        "operation": "UNION",
        "input": []
    }

    for query in node.operands:
        par["input"] += [handleSource(query)]
    return par


def defaultOperation(node):
    body = {
        'operation': node.operator.kind,
        'input': getColumnInfo(node.operands[0]),
        'output': node.operator.name
    }


def getColumnInfo(node):
    # check if its an array
    if hasattr(node, 'value'):
        return [node.value.stringValue]

    if(isinstance(node, list)):
        colInfo = []
        for col in node:
            colInfo += getColumnInfo(col)

        return colInfo

    if hasattr(node, 'operator'):
        return operatorFuncMap[node.operator.kind](node)

    if(hasattr(node, 'name')):
        return getColumnInfo(node.name)

    names = []
    for name in node.names:
        if(name == ''):
            names += ['*']
        else:
            names += [name]

    return names


def handleAs(node):
    body = {
        'operation': 'AS',
        'input': getColumnInfo(node.operands[0]),
        'output': getColumnInfo(node.operands[1])
    }

    return body


def handleFunc(node):
    body = {
        'operation': 'FUNCTION',
        'input': reduce(lambda x, y: x + [getColumnInfo(y)], node.operands, []),
        'output': node.operator.name
    }
    return body


def handleCreateTable(node):
    return handleSource(node.query)


operatorFuncMap = {
    'AS': handleAs,
    'OTHER_FUNCTION': handleFunc,
    "OTHER": handleFunc,
    "UNION": HandleUnion,
    "CREATE_TABLE": handleCreateTable
}


def handleOperation(node):
    type = node.operator.kind
    return operatorFuncMap[type](node)
