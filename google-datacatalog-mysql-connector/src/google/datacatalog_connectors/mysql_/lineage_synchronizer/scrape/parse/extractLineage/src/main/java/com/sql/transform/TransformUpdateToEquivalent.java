package com.sql.transform;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransformUpdateToEquivalent implements ITranformToEquivalent {
    public static void main(String[] args) throws Exception {
        System.out.println("started");
        String query = args[0];
        ITranformToEquivalent transformer = new TransformUpdateToEquivalent();
        String result = transformer.transform(query);

        System.out.println("result: " + result);
    }

    public static String filterSetVarWithDot(String query) {
        String varRegex = "[A-Za-z0-9_]+\\.[A-Za-z0-9_]+\\s*=";
        query = query.replaceAll(varRegex,"x=");

        return query;
    }

    @Override
    public Boolean canTransform(String query) {
        String updateRegex = "update\\s+((?s).*)SET((?s).*)";
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
        String updateRegex = "update\\s+((?s).*)SET((?s).*)";
        final Pattern pattern = Pattern.compile(updateRegex, Pattern.CASE_INSENSITIVE);
        // find Matching patterns
        Matcher matcher = pattern.matcher(query);
        matcher.find();

        String colListString = matcher.group(1);
        return colListString.split(",");
    }

    private String getSetQuery(String query) {
        String updateRegex = "update\\s+((?s).*)SET((?s).*)";
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

            equivalentQuery += "INSERT INTO " + target +" SELECT * FROM " + combinedSources + ";";
            equivalentQuery += "UPDATE  " + target + " SET " + setClosure + ";";
        }

        return equivalentQuery;
    }
}
