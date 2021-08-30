package com;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.*;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseSql {
    public static boolean hasLineage(String query) {
        return true;
    }

    final static String createStatmentRegex = "create\\s+table\\s+([a-zA-Z][a-zA-Z_1-9]*)\\s*(\\(((?s).*)\\))?\\sas((?s).*)";

    public static boolean isCreateStatement(String query) {
        // check if follows create table as pattern
        // and extract group
        final Pattern pattern = Pattern.compile(createStatmentRegex);
        return pattern.matcher(query).matches();
    }

    public static SqlNode parse(String query) throws SqlParseException {
        query = query.toLowerCase();
        // consider create table as possibility
        if (isCreateStatement(query)) {
            // TODO: use really positions
            final SqlParserPos dummyPos = new SqlParserPos(0, 0);
            final Pattern pattern = Pattern.compile(createStatmentRegex);

            // find Matching patterns
            Matcher matcher = pattern.matcher(query);
            matcher.find();

            String tableName = matcher.group(1);
            SqlIdentifier tableIdentfier = new SqlIdentifier(tableName, dummyPos);
            SqlNodeList sqlNodeList = new SqlNodeList(dummyPos);
            if (matcher.group(3) != null) {
                String schema = matcher.group(3);
                String[] columns = schema.split(",");

                for (String col : columns) {
                    // handle possible error here
                    String[] definetion = col.split(" ");
                    // TODO: add schema variables to the sqlNodeList
                    SqlIdentifier colIdentifier = new SqlIdentifier(definetion[0], dummyPos);
//                    org.apache.calcite.sql.SqlColu
                }

            }

            // parse the select sql separately
            String selectQuery = matcher.group(4);
            SqlParser par = SqlParser.create(selectQuery);
            SqlNode selectNode = par.parseStmtList();
            SqlOperator operator = new SqlOperator("CREATE", SqlKind.CREATE_TABLE, 0, 0, null, null, null) {
                @Override
                public SqlSyntax getSyntax() {
                    return null;
                }
            };

            SqlCreate createNode;
            createNode = new SpecialSqlCreateTable(dummyPos, false, false, tableIdentfier, sqlNodeList, selectNode);
            return createNode;
        }

        SqlParser par = SqlParser.create(query);
        return par.parseStmtList();
    }

    public static String parseSqlToJson(String query) throws Exception {
        // parse a select sql query
        SqlNode node = parse(query);
        // setup object mapper
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        return objectMapper.writeValueAsString(node);
    }
}
