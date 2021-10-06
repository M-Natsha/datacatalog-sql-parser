package com.gsql;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

public class ParseSql {

    public static void main(String[] args) throws Exception {
        System.out.println("started");

        String query = args[0];
        String result = parseSqlToJson(query);

        System.out.println("result: " + result);
    }

    public static SqlNode parse(String query) throws SqlParseException {
        // Change the parser configuration depending on Sql query type
        SqlParser.Config sqlParserConfig;

        sqlParserConfig = SqlParser.configBuilder()
                .setParserFactory(SqlDdlParserImpl.FACTORY)
                .setConformance(SqlConformanceEnum.MYSQL_5)
                .setLex(Lex.MYSQL)
                .build();

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
