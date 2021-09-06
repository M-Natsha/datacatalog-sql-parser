import subprocess
import os
import json
from types import SimpleNamespace
from lineage import extractLineage
import re

noLineageQueryPatterns = [
    re.compile("\\s*SET\\s+.*", re.IGNORECASE),
    re.compile("\\s*DROP\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*EXPLAIN\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*SHOW\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*DESC\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*DESCRIBE\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*DESCRIBE\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*use\\s+(.*)", re.IGNORECASE)
]

def hasLineage(query):
    global noLineageQueryPatterns

    for pattern in noLineageQueryPatterns:
        if pattern.match(query):
            return False

    # else it has lineage info
    return True

def parseQuery(query):
    path2jar = os.getcwd() + "/google-datacatalog-mysql-connector/extractLineage/target/test-1.0-SNAPSHOT-jar-with-dependencies.jar"
    x = subprocess.check_output(
        ["java", "-jar", path2jar, "-p", query], stderr=subprocess.STDOUT)  # doesn't capture output
    # from byte to string
    x = x.decode('ascii')

    # to json
    jsdata = json.loads(x, object_hook=lambda d: SimpleNamespace(**d))
    return extractLineage(jsdata[0])
