package com.sql.transform;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransformDeleteToEquivalent implements ITranformToEquivalent {
    public static void main(String[] args) {
        String query = args[0];
        ;

        ITranformToEquivalent transform = new TransformDeleteToEquivalent();
        if(transform.canTransform(query)) {
            System.out.println("can transform");
            query = transform.transform(query);
        }

        System.out.println(query);

    }
    @Override
    public Boolean canTransform(String query) {
        String deleteFromRegex = "\\s*DELETE\\s+((?s).*)";
        final Pattern pattern = Pattern.compile(deleteFromRegex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(query);
        if(!matcher.matches()) {
            return false;
        }

        String deleteBody = matcher.group(1);

        int fromIndex = Commons.findSqlKeyword(deleteBody, "\\sFROM\\s");

        if(fromIndex == -1) {
            return false;
        }

        String targetTable = deleteBody.substring(0,fromIndex).trim();
        if(targetTable.equals("")) {
            return false;
        }

        return true;
    }

    @Override
    public String transform(String query) {
        String deleteRegex = "\\s*DELETE\\s+((?s).*)";
        final Pattern pattern = Pattern.compile(deleteRegex, Pattern.CASE_INSENSITIVE);
        // find Matching patterns
        Matcher matcher = pattern.matcher(query);
        matcher.find();

        String deleteBody = matcher.group(1);
        int fromStart = Commons.findSqlKeyword(deleteBody, "\\s?FROM\\s");
        String targetTable = deleteBody.substring(0,fromStart).trim();
        String subQuery = deleteBody.substring(fromStart);
        String equivalentQuery = "UPDATE " + targetTable + " \n "
                + " SET _ = (SELECT * " + subQuery + ")";

        ITranformToEquivalent updateTransform = new TransformUpdateToEquivalent();

        if (updateTransform.canTransform(equivalentQuery)) {
            equivalentQuery = updateTransform.transform(equivalentQuery);
        }
        return  equivalentQuery;
    }
}
