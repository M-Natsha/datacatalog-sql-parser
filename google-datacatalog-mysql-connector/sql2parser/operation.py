from functools import reduce

def handleAs(node):
    body = {
        'operation': 'AS',
        'input' : node.operands[0].names[0],
        'output': node.operands[1].names[0] 
    }
    
    return body

def handleFunc(node):
    body = {
        'operation': 'FUNCTION',
        'input' : reduce(lambda x,y:x + y.names, node.operands, []),
        'output': node.operator.name
    }
    return body

operatorFuncMap = {
    'AS' : handleAs,
    'OTHER_FUNCTION': handleFunc
}
