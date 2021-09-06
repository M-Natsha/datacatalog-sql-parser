from parseSql import *
from collections import OrderedDict


def getTableLineageFromNode(node):

    if(isinstance(node, str)):
        return node

    if(isinstance(node, list)):
        tableArray = []
        for source in node:
            source = getTableLineageFromNode(source)
            if isinstance(source, list):
                tableArray += source
            else:
                tableArray += [source]

        return list(dict.fromkeys(tableArray))

    if('target' in node and 'source' in node):
        return {
            'target':  getTableLineageFromNode(node['target']),
            'source': getTableLineageFromNode(node['source'])
        }

    # TODO: unite table format
    if('tables' in node):
        return getTableLineageFromNode(node['tables'])

    if('table' in node):
        return getTableLineageFromNode(node['table'])

    if('operation' in node):
        return getTableLineageFromNode(node['input'])


def getTableLineage(query):
    parseTree = parseQuery(query)
    return getTableLineageFromNode(parseTree)


result = getTableLineage("SELECT * FROM t1")
print(result)
