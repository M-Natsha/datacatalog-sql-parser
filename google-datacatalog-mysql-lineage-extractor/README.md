# google-datacatalog-mysql-linage-extractor

Library for extracting lineage from MySQL.


**Disclaimer: This is not an officially supported Google product.**

<!--
  ⚠️ DO NOT UPDATE THE TABLE OF CONTENTS MANUALLY ️️⚠️
  run `npx markdown-toc -i README.md`.

  Please stick to 80-character line wraps as much as you can.
-->
-----

## 1. Installation
### 1.1. requirements

- jde 1.8
- maven
- python 3.7+
- pip

### 1.2. Installation steps

1. build calcite-sql-parser jar file

```bash
cd ./calcite-sql-parser
mvn clean compile assembly:single
```

2. add the jar to lineage extractor source
```bash
mv ./calcite-sql-parser/target/sql-parser-0.1-jar-with-dependencies.jar \
./google-datacatalog-mysql-lineage-extractor/src/jars/sql-parser.jar
```

3. create virutal env and install lineage-extractor

```bash
pip3 install virtualenv
virtualenv --python python3.7 <your-env>
source <your-env>/bin/activate
<your-env>/bin/pip install google-datacatalog-mysql-lineage-extractor
```

## 2. Run entry points

### 4.1. extract lineage from query entry point

within the virtualenv

```bash
google-datacatalog-mysql-extract-lineage <SQL_QUERY>
```

### 4.2. extract lineage from MySQL database entry point

```bash
google-datacatalog-mysql-db-extract-lineage \
--mysql-host=<HOST> \
--mysql-user=<USER> \
--mysql-pass=<PASSWORD>
```

## 3. Developer environment
### 6.1. Install and run Yapf formatter

```bash
pip install --upgrade yapf

# Auto update files
yapf --in-place --recursive src tests

# Show diff
yapf --diff --recursive src tests

# Set up pre-commit hook
# From the root of your git project.
curl -o pre-commit.sh https://raw.githubusercontent.com/google/yapf/master/plugins/pre-commit.sh
chmod a+x pre-commit.sh
mv pre-commit.sh .git/hooks/pre-commit
```

### 3.2. Install and run Flake8 linter

```bash
pip install --upgrade flake8
flake8 src tests
```

### 3.3. Run Tests

```bash
python setup.py test
```