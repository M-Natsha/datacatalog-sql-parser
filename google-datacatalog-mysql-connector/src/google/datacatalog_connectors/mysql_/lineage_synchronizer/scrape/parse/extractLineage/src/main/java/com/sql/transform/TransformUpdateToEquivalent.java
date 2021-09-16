package com.sql.transform;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransformUpdateToEquivalent implements ITranformToEquivalent {
    public static void main(String[] args) throws Exception {
        System.out.println("started");
//        String query = args[0];
        String query = "UPDATE t1, (SELECT * FROM t2) as t3 SET t1,";
        ITranformToEquivalent transformer = new TransformUpdateToEquivalent();
        String result = transformer.transform(query);

        System.out.println("result: " + result);
    }

    @Override
    public Boolean canTransform(String query) {
        String updateRegex = "update((?s).*)\\,((?s).*)SET((?s).*)";
        final Pattern pattern = Pattern.compile(updateRegex, Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    @Override
    public String transform(String query) {
        String updateRegex = "update((?s).*)SET((?s).*)";
        final Pattern pattern = Pattern.compile(updateRegex, Pattern.CASE_INSENSITIVE);
        // find Matching patterns
        Matcher matcher = pattern.matcher(query);
        matcher.find();

        String colListString = matcher.group(1);
        String[] colList = colListString.split(",");

        String equivalentQuery = "";
        for (String col : colList) {
            if (col.trim().equalsIgnoreCase("")) {
                continue;
            }

            equivalentQuery += "SELECT * FROM " + col + ";";

        }

        return equivalentQuery;
    }
}
