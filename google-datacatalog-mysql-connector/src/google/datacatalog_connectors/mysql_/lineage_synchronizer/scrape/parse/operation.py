from functools import reduce
from enum import Enum
from iteration_utilities import unique_everseen 

class Scope(Enum):
    COLUMN = 1
    TABLE = 2
    DATABASE = 3


def remove_duplicates(myList: list):
    return list(unique_everseen(myList))

def compress(nodeList):
    if len(nodeList) <= 1:
        return nodeList
    
    if not any('tables' in node for node in nodeList):
        return nodeList
    
    result = {
        'tables': [],
        'columns': []
    }
    
    nodeList = remove_duplicates(nodeList)
    for node in nodeList:
        if('tables' in node):
            result['tables'] += node['tables']
            result['columns'] += node['columns']
        else:
            AppendOrExtend(result['tables'],node)
    
    result['tables'] = remove_duplicates(result['tables'])
    result['columns'] = remove_duplicates(result['columns'])

    return result

def AppendOrExtend(myList, item):
    if isinstance(item,list):
        myList.extend(item)
    else:
        myList.append(item)

    return myList

def handleJoin(node):
    par = {"operation": "JOIN", "input": []}

    scope = Scope.TABLE
    if hasattr(node, 'left'):
        AppendOrExtend(par['input'], handleSource(node.left,scope))

    if hasattr(node, 'right'):
        AppendOrExtend(par['input'], handleSource(node.right, scope))
        
    return par


def handleFrom(node):
    frm = getattr(node, 'from')

    source = {
        'tables': handleSource(frm, Scope.TABLE),
        'columns': reduce(lambda x, y: AppendOrExtend(x, getColumnInfo(y, Scope.TABLE)), node.selectList,
                          [])
    }

    return source


def handleSource(node, scope):
    if node is None:
        return []
    
    sources = []
    if isinstance(node,list):
        for element in node:
            AppendOrExtend(sources, handleSource(element, scope))
        
    if hasattr(node, 'source'):
        AppendOrExtend(sources, handleSource(node.source, scope))

    # return names
    if hasattr(node, 'names'):
        AppendOrExtend(sources,  getColumnInfo(node, scope))

    # handle from
    if hasattr(node, 'from'):
        AppendOrExtend(sources,  handleFrom(node))

    if hasattr(node, 'operator'):
        AppendOrExtend(sources,  handleOperation(node, scope))
    
    if hasattr(node, 'left'):
        AppendOrExtend(sources,  handleJoin(node))

    if hasattr(node, 'query'):
        AppendOrExtend(sources,  handleSource(node.query, scope))

    if(hasattr(node, 'where')):
        AppendOrExtend(sources,  handleSource(node.where, Scope.COLUMN))
    
    if(hasattr(node, 'condition')):
        AppendOrExtend(sources,  handleSource(node.condition, Scope.COLUMN))
      
    if(hasattr(node, 'sourceExpressionList')):
        print("source expression is here")
        AppendOrExtend(sources,  handleSource(node.sourceExpressionList, Scope.COLUMN))
        
    
    return compress(sources)


def HandleUnion(node, scope):
    par = {"operation": "UNION", "input": []}

    for query in node.operands:
        AppendOrExtend(par["input"], handleSource(query, Scope.TABLE))
    return par


def defaultOperation(node, scope):
    return {
        'operation': node.operator.kind,
        'input': getColumnInfo(node.operands[0], scope),
        'output': node.operator.name
    }


def getColumnInfo(node, scope):
    # check if its an array
    if hasattr(node, 'value'):
        return [node.value.stringValue]

    if isinstance(node, list):
        colInfo = []
        for col in node:
            AppendOrExtend(colInfo, getColumnInfo(col, scope))

        return colInfo

    if hasattr(node, 'name'):
        return getColumnInfo(node.name, scope)

    if hasattr(node, 'names'):
        node.names = [name for name in node.names if name != "_"]
        
        if len(node.names) == 0:
            return []
        
        if scope == Scope.TABLE:    
            result = ""
            for name in node.names:
                if result != "":
                    result += "."
                
                if name == '':
                    result += '*'
                else:                    
                    result += name
        else:
            result = {
                "Operation": "GET_COLUMN",
                "input": [],
                "output": []
            }
            
            lastName = node.names[-1]
            result["output"] = ['*' if lastName == '' else lastName]
            
            if len(node.names) > 1:
                tableName = ""
                for name in node.names[:-1]:
                    if tableName != "":
                        tableName += "."
                    
                    tableName += name
                    
                if tableName != '':
                    result['input'] = [tableName]
            
        return result
    
    return handleSource(node, scope)


def handleAs(node, scope):
    body = {
        'operation': 'AS',
        'input': getColumnInfo(node.operands[0], scope),
        'output': getColumnInfo(node.operands[1], scope)
    }

    return body


def handleFunc(node, scope):
    body = {
        'operation': 'FUNCTION',
        'input': reduce(lambda x, y: AppendOrExtend(x, getColumnInfo(y, scope)) , node.operands,
                        []),
        'output': node.operator.name
    }
    return body


def handle_create_table(node, scope):
    return handleSource(node.query, scope)


operatorFuncMap = {
    'AS': handleAs,
    'OTHER_FUNCTION': handleFunc,
    "OTHER": handleFunc,
    "UNION": HandleUnion,
    "CREATE_TABLE": handle_create_table
}


def handleOperation(node, scope):
    type = node.operator.kind
    if type in operatorFuncMap:
        return operatorFuncMap[type](node, scope)
    
    if not hasattr(node,'operands'):
        return []
    
    sources = []
    for operand in node.operands:
        sources = AppendOrExtend(sources, handleSource(operand,scope))
        
    return sources