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

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseSql {

    public static void main(String[] args) throws Exception {
        System.out.println("started");
        String query = args[0];

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

    public static boolean isCreateOperation(String query) {
        String updateRegex = "\\s*CREATE\\s+TABLE\\s+((?s).*)";
        final Pattern pattern = Pattern.compile(updateRegex, Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    public static String preprocessQuery(String query) {
        // Replace " with '
        query = query.replace('\"', '\'');
        return query;
    }

    public static boolean IsDeleteFrom(String query) {
        String deleteFromRegex = "DELETE\\s+(\\S+\\s+)FROM((?s).*)";
        final Pattern pattern = Pattern.compile(deleteFromRegex, Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
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

            if(isCreateOperation(query)) {
                int start = -1, end = -1;
                int counter = 0;
                for(int i=0;i < query.length();i++) {
                    char c = query.charAt(i);
                    if(c == '(') {
                        start = start == -1? i: start;
                        counter++;
                    } else if(c == ')') {
                        counter--;
                        if(counter == 0) {
                            end = i;
                            break;
                        }
                    }
                }

                if(start != -1 && end != -1) {
                    String columnDef = query.substring(start + 1, end);
                    String[] columns = columnDef.split(",");

                    String newColDef = "";
                    for (String col: columns) {
                        if(!col.trim().equals("") && !newColDef.equals("")) {
                            newColDef += ",";
                        }

                        String arr[] = col.trim().split(" ", 2);

                        if(!arr[0].equals(")")){
                            newColDef += arr[0];
                        }
                    }

                    query = query.replace(query.substring(start + 1, end), newColDef);
                }

            }

            SqlParser par = SqlParser.create(query, sqlParserConfig);

            return par.parseStmtList();
        }

        SqlParser.Config sqlParserConfig = SqlParser.configBuilder()
                .setConformance(SqlConformanceEnum.MYSQL_5)
                .setLex(Lex.MYSQL)
                .build();

        if(IsDeleteFrom(query)) {
            String deleteRegex = "DELETE\\s+(\\S+\\s+)?FROM(.*)";
            final Pattern pattern = Pattern.compile(deleteRegex, Pattern.CASE_INSENSITIVE);
            // find Matching patterns
            Matcher matcher = pattern.matcher(query);
            matcher.find();

            String targetTable = matcher.group(1);
            String subQuery = matcher.group(2);
            String equivalentQuery = "INSERT INTO " + targetTable + " \n "
                    + "SELECT * FROM " + subQuery;
            SqlParser par = SqlParser.create(equivalentQuery, sqlParserConfig);
            return par.parseStmtList();
        }

        // Handling update is some special case
        // This Update works only with MySQL
//       if(isUpdateFromQuery(query)) {
//           System.out.println("it is update from");
//           String updateRegex = "update((?s).*)SET((?s).*)";
//           final Pattern pattern = Pattern.compile(updateRegex, Pattern.CASE_INSENSITIVE);
//           // find Matching patterns
//           Matcher matcher = pattern.matcher(query);
//           matcher.find();
//
//           String query1 = "SELECT * FROM " + matcher.group(1);
//           String query2 = "UPDATE dummy_target SET " +  matcher.group(2);
//
//           System.out.println(query1);
//           System.out.println(query2);
//           SqlParser par1 = SqlParser.create(query1, sqlParserConfig);
//           SqlParser par2 = SqlParser.create(query2, sqlParserConfig);
//
//           SqlSelect node1 = (SqlSelect)par1.parseStmt();
//           SqlUpdate node2 = (SqlUpdate)par2.parseStmt();
//
////           node2node1.getSelectList();
//
//           ObjectMapper objectMapper = new ObjectMapper();
//           objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
//           objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
//           objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
//
//           try {
//               String t1 = objectMapper.writeValueAsString(node1);
//               String t2 = objectMapper.writeValueAsString(node2);
//
//               System.out.println(t1);
//               System.out.println(t2);
//           } catch (JsonProcessingException e) {
//               e.printStackTrace();
//           }
//
//           // TODO merge the 2 queries results
//           return node1;
//       }

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
