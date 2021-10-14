package com.sql.transform;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransformDeleteToEquivalent implements ITranformToEquivalent {
    @Override
    public Boolean canTransform(String query) {
        String deleteFromRegex = "DELETE\\s+(\\S+\\s+)FROM((?s).*)";
        final Pattern pattern = Pattern.compile(deleteFromRegex, Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    @Override
    public String transform(String query) {
        String deleteRegex = "DELETE\\s+(\\S+\\s+)?FROM(.*)";
        final Pattern pattern = Pattern.compile(deleteRegex, Pattern.CASE_INSENSITIVE);
        // find Matching patterns
        Matcher matcher = pattern.matcher(query);
        matcher.find();

        String targetTable = matcher.group(1);
        String subQuery = matcher.group(2);
        String equivalentQuery = "INSERT INTO " + targetTable + " \n "
                + "SELECT * FROM " + subQuery;

        return  equivalentQuery;
    }
}
