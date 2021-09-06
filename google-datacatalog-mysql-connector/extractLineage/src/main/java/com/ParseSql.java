package com;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
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

    public static void main(String[] args) throws Exception {
        System.out.println("started");
        String query = "UPDATE target\n" +
                "SET\n" +
                "    targetTable.t1  = sourceTable.cola,\n" +
                "    targetTable.t2 = sourceTable.colb, \n" +
                "    targetTable.3 = sourceTable.colc, \n" +
                "    targetTable.t4 = sourceTable.cold ";

        String result = parseSqlToJson(query);

        System.out.println("result: " + result);
    }

    public static boolean hasLineage(String query) {
        return true;
    }

    public static boolean isUpdateFromQuery(String query) {
        String updateRegex = "update((?s).*)\\,((?s).*)SET((?s).*)";
        final Pattern pattern = Pattern.compile(updateRegex, Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
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
    public static String preprocessQuery(String query) {
        // Replace " with '
        query = query.replace('\"', '\'');
        return query;
    }

    public static SqlNode parse(String query) throws SqlParseException {

        query = preprocessQuery(query);
        // Change the parser depending on Sql query type
        if(isDdlOperation(query)) {
            SqlParser.Config sqlParserConfig = SqlParser.configBuilder()
                    .setParserFactory(SqlDdlParserImpl.FACTORY)
                    .setConformance(SqlConformanceEnum.MYSQL_5)
                    .setLex(Lex.MYSQL)
                    .build();
            SqlParser par = SqlParser.create(query, sqlParserConfig);
            return par.parseStmtList();

        }

        SqlParser.Config sqlParserConfig = SqlParser.configBuilder()
                .setConformance(SqlConformanceEnum.MYSQL_5)
                .setLex(Lex.MYSQL)
                .build();

        // Handling update is some special case
        // This Update works only with MySQL
       if(isUpdateFromQuery(query)) {
           System.out.println("it is update from");
           String updateRegex = "update((?s).*)SET((?s).*)";
           final Pattern pattern = Pattern.compile(updateRegex, Pattern.CASE_INSENSITIVE);
           // find Matching patterns
           Matcher matcher = pattern.matcher(query);
           matcher.find();

           String query1 = "SELECT * FROM FROM " + matcher.group(1);
           String query2 = "UPDATE dummy_target SET " +  matcher.group(2);

           SqlParser par1 = SqlParser.create(query1, sqlParserConfig);
           SqlParser par2 = SqlParser.create(query2, sqlParserConfig);

           SqlNode node1 = par1.parseStmtList();
           SqlNode node2 = par2.parseStmtList();

           ObjectMapper objectMapper = new ObjectMapper();
           objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
           objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
           objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

           try {
               String t1 = objectMapper.writeValueAsString(node1);
               String t2 = objectMapper.writeValueAsString(node2);

               System.out.println(t1);
               System.out.println(t2);
           } catch (JsonProcessingException e) {
               e.printStackTrace();
           }


           // TODO merge the 2 queries results
           return node1;
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
