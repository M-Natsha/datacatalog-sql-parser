from functools import reduce
from .operation \
    import append_or_extend, Scope, get_col_info, handle_source
from iteration_utilities import unique_everseen


def get_col_with_unknown_tables(node):
    if isinstance(node, str):
        return []

    if isinstance(node, list):
        result = []
        for source in node:
            col = get_col_with_unknown_tables(source)
            if col is not None:
                append_or_extend(result, col)

        return result

    if 'target' in node and 'source' in node:
        return get_col_with_unknown_tables(node['source'])

    if 'tables' in node:
        return get_col_with_unknown_tables(node['tables'])

    if 'Operation' in node:
        if node['Operation'] == "GET_COLUMN":
            if len(node['input']) == 0:
                return node['output']
            else:
                return []
        else:
            return get_col_with_unknown_tables(node['input'])


def handle_insert(node):
    # extract target data
    target = {
        "tables": [get_col_info(node.targetTable, Scope.TABLE)],
        "columns": []
    }

    if hasattr(node, 'targetColumnList') and len(node.targetColumnList) > 0:
        append_or_extend(target["columns"],
                         get_col_info(node.targetColumnList, Scope.TABLE))

    if len(target["columns"]) == 0:
        target["columns"] = ['*']

    if hasattr(node, 'columnList') and node.columnList is not None:
        target['columns'] = reduce(
            lambda x, y: append_or_extend(x, get_col_info(y, Scope.TABLE)),
            node.columnList, [])

    # exctract source data
    source = handle_source(node, Scope.TABLE)

    return {"source": source, "target": target}


def handle_select(node):
    return {"source": handle_source(node, Scope.TABLE)}


def handle_query(node):
    return extract_lineage(node.query)


def handle_create_table(node):
    res = {
        "target": {
            "tables": [],
            "columns": []
        },
        "source": handle_source(node, Scope.TABLE)
    }

    res['target']['tables'] = [get_col_info(node.name, Scope.TABLE)]

    if hasattr(node, 'columnList') and node.columnList is not None:
        res['target']['columns'] = reduce(
            lambda x, y: x + [get_col_info(y, Scope.TABLE)], node.columnList,
            [])
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
    """removes duplicates from a list and reserve its order"""
    return list(unique_everseen(myList))


def combine_lineage_events(*lineageEvents: dict):
    combinedEvents = {'target': [], 'source': []}

    for event in lineageEvents:
        if 'target' in event:
            target = event['target']
            if not isinstance(target, list):
                target = [event['target']]

            combinedEvents['target'] += target

        if 'source' in event:
            source = event['source']
            if not isinstance(source, list):
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
