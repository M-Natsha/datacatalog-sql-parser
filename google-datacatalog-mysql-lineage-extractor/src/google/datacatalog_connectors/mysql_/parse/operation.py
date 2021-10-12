from functools import reduce
from enum import Enum
from iteration_utilities import unique_everseen


class Scope(Enum):
    """Describes the type of the variabe. we parse on the query"""
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

    result = {'tables': [], 'columns': []}

    nodeList = remove_duplicates(nodeList)
    for node in nodeList:
        if ('tables' in node):
            result['tables'] += node['tables']
            result['columns'] += node['columns']
        else:
            append_or_extend(result['tables'], node)

    result['tables'] = remove_duplicates(result['tables'])
    result['columns'] = remove_duplicates(result['columns'])

    return result


def append_or_extend(myList: list, item):
    """Returns and modifis a list by appending an item to the list
    or merging the item with the list if the item is also a list

    Args:
        myList ([type]): [description]
        item ([type]): [description]

    Returns:
        [type]: [description]
    """
    if isinstance(item, list):
        myList.extend(item)
    else:
        myList.append(item)

    return myList


def handle_join(node):
    par = {"operation": "JOIN", "input": []}

    scope = Scope.TABLE
    if hasattr(node, 'left'):
        append_or_extend(par['input'], handle_source(node.left, scope))

    if hasattr(node, 'right'):
        append_or_extend(par['input'], handle_source(node.right, scope))

    return par


def handle_from(node):
    frm = getattr(node, 'from')

    source = {
        'tables':
            handle_source(frm, Scope.TABLE),
        'columns':
            reduce(
                lambda x, y: append_or_extend(x, get_col_info(y, Scope.TABLE)),
                node.selectList, [])
    }

    return source


def handle_source(node, scope):
    """parses Apache Calcite Node and extract information related to

    Args:
        node ([type]): [description]
        scope ([type]): [description]

    Returns:
        [type]: [description]
    """
    if node is None:
        return []

    sources = []
    if isinstance(node, list):
        for element in node:
            append_or_extend(sources, handle_source(element, scope))

    if hasattr(node, 'source'):
        append_or_extend(sources, handle_source(node.source, scope))

    # return names
    if hasattr(node, 'names'):
        append_or_extend(sources, get_col_info(node, scope))

    # handle from
    if hasattr(node, 'from'):
        append_or_extend(sources, handle_from(node))

    if hasattr(node, 'operator'):
        append_or_extend(sources, handle_operation(node, scope))

    if hasattr(node, 'left'):
        append_or_extend(sources, handle_join(node))

    if hasattr(node, 'query'):
        append_or_extend(sources, handle_source(node.query, scope))

    if (hasattr(node, 'where')):
        append_or_extend(sources, handle_source(node.where, Scope.COLUMN))

    if (hasattr(node, 'condition')):
        append_or_extend(sources, handle_source(node.condition, Scope.COLUMN))

    if (hasattr(node, 'sourceExpressionList')):
        append_or_extend(
            sources, handle_source(node.sourceExpressionList, Scope.COLUMN))

    return compress(sources)


def handle_union(node, scope):
    par = {"operation": "UNION", "input": []}

    for query in node.operands:
        append_or_extend(par["input"], handle_source(query, Scope.TABLE))
    return par


def get_col_info(node, scope):
    # check if its an array
    if hasattr(node, 'value'):
        return [node.value.stringValue]

    if isinstance(node, list):
        colInfo = []
        for col in node:
            append_or_extend(colInfo, get_col_info(col, scope))

        return colInfo

    if hasattr(node, 'name'):
        return get_col_info(node.name, scope)

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
            result = {"Operation": "GET_COLUMN", "input": [], "output": []}

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

    return handle_source(node, scope)


def handle_as(node, scope):
    body = {
        'operation': 'AS',
        'input': get_col_info(node.operands[0], scope),
        'output': get_col_info(node.operands[1], scope)
    }

    return body


def handle_func(node, scope):
    body = {
        'operation':
            'FUNCTION',
        'input':
            reduce(lambda x, y: append_or_extend(x, get_col_info(y, scope)),
                   node.operands, []),
        'output':
            node.operator.name
    }
    return body


def handle_create_table(node, scope):
    return handle_source(node.query, scope)


operatorFuncMap = {
    'AS': handle_as,
    'OTHER_FUNCTION': handle_func,
    "OTHER": handle_func,
    "UNION": handle_union,
    "CREATE_TABLE": handle_create_table
}


def handle_operation(node, scope):
    type = node.operator.kind
    if type in operatorFuncMap:
        return operatorFuncMap[type](node, scope)

    if not hasattr(node, 'operands'):
        return []

    sources = []
    for operand in node.operands:
        sources = append_or_extend(sources, handle_source(operand, scope))

    return sources
