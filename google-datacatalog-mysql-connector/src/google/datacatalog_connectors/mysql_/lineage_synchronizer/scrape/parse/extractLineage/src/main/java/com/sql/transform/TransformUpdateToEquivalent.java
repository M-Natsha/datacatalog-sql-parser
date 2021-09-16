package com.sql.transform;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransformUpdateToEquivalent implements ITranformToEquivalent {
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

        String query1 = "SELECT * FROM " + matcher.group(1);
        String query2 = "UPDATE dummy_target SET " +  matcher.group(2);

        return query1 + ";" + query2;
    }
}
