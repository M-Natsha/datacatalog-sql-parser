import subprocess
import os
import pathlib
import json
from types import SimpleNamespace
from lineage import extractLineage


class Object:
    pass


def getTestQuery(path):
    scriptDir = pathlib.Path(__file__).parent.resolve()
    f = open(str(scriptDir) + "/tests/" + path, "r")
    return f.read()


sqlQuery = getTestQuery("test5.sql")

path2jar = os.getcwd() + "/google-datacatalog-mysql-connector/extractLineage/target/test-1.0-SNAPSHOT-jar-with-dependencies.jar"
x = subprocess.check_output(
    ["java", "-jar", path2jar, "-p", sqlQuery])  # doesn't capture output
# from byte to string
x = x.decode('ascii')
# to json
jsdata = json.loads(x, object_hook=lambda d: SimpleNamespace(**d))

print(x)


for sqlQuery in jsdata:
    lineage = extractLineage(sqlQuery)
    print(lineage)

# lineage = extractLineage(jsdata)
# print(lineage)
