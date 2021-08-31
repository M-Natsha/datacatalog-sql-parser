package com;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.*;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseSql {
    public static boolean hasLineage(String query) {
        return true;
    }

    public static boolean isDdlOperation(String query) {
        String[] ddlRegex = {
                "\\s*create\\s+((?s).*)" // Create query
                ,"\\s*alter\\s+((?s).*)" // Alter query
                ,"\\s*drop\\s+((?s).*)" // Drop query
        };

        for(String validator: ddlRegex) {
            final Pattern pattern = Pattern.compile(validator, Pattern.CASE_INSENSITIVE);
            if(pattern.matcher(query).matches()) {
                return true;
            }
        }

        return false;
    }

    public static SqlNode parse(String query) throws SqlParseException {
        SqlParser par;
        // Change the parser depending on Sql query type
        if(isDdlOperation(query)) {
            SqlParser.Config sqlParserConfig = SqlParser.configBuilder()
                    .setParserFactory(SqlDdlParserImpl.FACTORY)
                    .setConformance(SqlConformanceEnum.MYSQL_5)
                    .setLex(Lex.MYSQL)
                    .build();
            par = SqlParser.create(query, sqlParserConfig);
        } else {
            SqlParser.Config sqlParserConfig = SqlParser.configBuilder()
                    .setConformance(SqlConformanceEnum.MYSQL_5)
                    .setLex(Lex.MYSQL)
                    .build();

            par = SqlParser.create(query, sqlParserConfig);
        }

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
