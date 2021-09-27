package com;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.sql.transform.ITranformToEquivalent;
import com.sql.transform.TransformCreateToEquivalent;
import com.sql.transform.TransformDeleteToEquivalent;
import com.sql.transform.TransformUpdateToEquivalent;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.util.regex.Pattern;

public class ParseSql {

    public static void main(String[] args) throws Exception {
        System.out.println("started");

        String query = args[0];
        String result = parseSqlToJson(query);

        System.out.println("result: " + result);
    }

    public static boolean isDdlOperation(String query) {
        String[] ddlRegex = {
                "\\s*create\\s+((?s).*)" // Create query
                , "\\s*alter\\s+((?s).*)" // Alter query
                , "\\s*drop\\s+((?s).*)" // Drop query
        };

        for (String validator : ddlRegex) {
            final Pattern pattern = Pattern.compile(validator, Pattern.CASE_INSENSITIVE);
            if (pattern.matcher(query).matches()) {
                return true;
            }
        }

        return false;
    }

    public static String preprocessQuery(String query) {
        // Replace " with '
        query = query.replace('\"', '\'');
        return query;
    }

    public static SqlNode parse(String query) throws SqlParseException {

        query = preprocessQuery(query);
        // Change the parser configuration depending on Sql query type
        SqlParser.Config sqlParserConfig;
        if (isDdlOperation(query)) {
            sqlParserConfig = SqlParser.configBuilder()
                    .setParserFactory(SqlDdlParserImpl.FACTORY)
                    .setConformance(SqlConformanceEnum.MYSQL_5)
                    .setLex(Lex.MYSQL)
                    .build();
        } else {
            sqlParserConfig = SqlParser.configBuilder()
                    .setConformance(SqlConformanceEnum.MYSQL_5)
                    .setLex(Lex.MYSQL)
                    .build();
        }


        ITranformToEquivalent deleteTransform = new TransformDeleteToEquivalent();
        ITranformToEquivalent updateTransform = new TransformUpdateToEquivalent();
        TransformCreateToEquivalent createEqu = new TransformCreateToEquivalent();

        if (deleteTransform.canTransform(query)) {
            query = deleteTransform.transform(query);
        } else if (updateTransform.canTransform(query)) {
            query = updateTransform.transform(query);
        } else if (createEqu.canTransform(query)) {
            query = createEqu.transform(query);
        }

        SqlParser par = SqlParser.create(query, sqlParserConfig);
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
