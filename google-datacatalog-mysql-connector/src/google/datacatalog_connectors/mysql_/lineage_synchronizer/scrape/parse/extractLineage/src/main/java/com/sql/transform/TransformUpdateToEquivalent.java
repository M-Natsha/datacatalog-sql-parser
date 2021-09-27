package com.sql.transform;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransformUpdateToEquivalent implements ITranformToEquivalent {
    public static void main(String[] args) throws Exception {
        System.out.println("started");
        String query = "UPDATE messages , usersmessages \n" +
                "  SET _ = (SELECT *  FROM messages  INNER JOIN usersmessages  \n" +
                "WHERE messages.messageid= usersmessages.messageid and messages.messageid = '1')";
        ITranformToEquivalent transformer = new TransformUpdateToEquivalent();
        if(transformer.canTransform(query)) {
            System.out.println("can transform");
            query = transformer.transform(query);
        }
        System.out.println("result: " + query);
    }

    public static String filterSetVarWithDot(String query) {
        String setBody = "";

        int setPos = Commons.findSqlKeyword(query,"SET");
        int wherePos = Commons.findSqlKeyword(query, "WHERE");

        System.out.println((wherePos));
        if(setPos != -1) {
            if(wherePos != -1) {
                setBody = query.substring(setPos + 5, wherePos);
            } else {
                setBody = query.substring(setPos);
            }
        }

        String varRegex = "[A-Za-z0-9_]+\\.[A-Za-z0-9_]+\\s*=";
        String newBody = setBody.replaceAll(varRegex,"_ =");

        query = query.replace(setBody,newBody);
        return query;
    }

    @Override
    public Boolean canTransform(String query) {
        String updateRegex = "\\s*update\\s+((?s).*)SET((?s).*)";
        final Pattern pattern = Pattern.compile(updateRegex, Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    private Boolean isSelectASQuery(String query) {
        query = query.trim();

        String updateRegex = "\\(\\s*SELECT\\s+(?s).*as\\s+[a-z0-9_]*";
        final Pattern pattern = Pattern.compile(updateRegex, Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    private String[] getColList(String query) {
        String updateRegex = "\\s*update\\s+((?s).*)SET((?s).*)";
        final Pattern pattern = Pattern.compile(updateRegex, Pattern.CASE_INSENSITIVE);
        // find Matching patterns
        Matcher matcher = pattern.matcher(query);
        matcher.find();

        String colListString = matcher.group(1);
        return colListString.split(",");
    }

    private String getSetQuery(String query) {
        String updateRegex = "\\s*update\\s+((?s).*)SET((?s).*)";
        final Pattern pattern = Pattern.compile(updateRegex, Pattern.CASE_INSENSITIVE);
        // find Matching patterns
        Matcher matcher = pattern.matcher(query);
        matcher.find();

        return matcher.group(2);
    }



    private  String[] getTargets(String[] colList) {
        return Arrays.stream(colList).filter(x -> !isSelectASQuery(x)).toArray(String[]::new);
    }

    @Override
    public String transform(String query) {
        // Filtering and removing unparsable information
        query = filterSetVarWithDot(query);

        String[] sources = getColList(query);
        String[] targets = getTargets(sources);

        String combinedSources = String.join(" JOIN ", sources);
        String setClosure = getSetQuery(query);

        String equivalentQuery = "";
        for (String target : targets) {
            if (target.trim().equalsIgnoreCase("")) {
                continue;
            }

            equivalentQuery += "UPDATE  " + target + " SET _ = ( SELECT * FROM " + combinedSources + ")," + setClosure + ";" ;
        }

        return equivalentQuery;
    }
}
