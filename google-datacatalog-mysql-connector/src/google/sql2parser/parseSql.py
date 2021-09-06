import subprocess
import os
import json
from types import SimpleNamespace
from lineage import extractLineage


def parseQuery(query):
    path2jar = os.getcwd() + "/google-datacatalog-mysql-connector/extractLineage/target/test-1.0-SNAPSHOT-jar-with-dependencies.jar"
    x = subprocess.check_output(
        ["java", "-jar", path2jar, "-p", query])  # doesn't capture output
    # from byte to string
    x = x.decode('ascii')

    # to json
    jsdata = json.loads(x, object_hook=lambda d: SimpleNamespace(**d))
    return extractLineage(jsdata[0])
