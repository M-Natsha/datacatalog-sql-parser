from functools import reduce


def getColumnInfo(node):
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
        'input': reduce(lambda x, y: x + getColumnInfo(y), node.operands, []),
        'output': node.operator.name
    }
    return body


operatorFuncMap = {
    'AS': handleAs,
    'OTHER_FUNCTION': handleFunc
}
