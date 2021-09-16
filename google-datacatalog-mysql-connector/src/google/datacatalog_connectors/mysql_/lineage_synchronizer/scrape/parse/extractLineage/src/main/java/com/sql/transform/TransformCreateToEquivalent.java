package com.sql.transform;

import java.util.regex.Pattern;

public class TransformCreateToEquivalent implements ITranformToEquivalent {
    @Override
    public Boolean canTransform(String query) {
        String updateRegex = "\\s*CREATE\\s+TABLE\\s+((?s).*)";
        final Pattern pattern = Pattern.compile(updateRegex, Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();

    }

    @Override
    public String transform(String query) {
        int start = -1, end = -1;
        int counter = 0;
        for (int i = 0; i < query.length(); i++) {
            char c = query.charAt(i);
            if (c == '(') {
                start = start == -1 ? i : start;
                counter++;
            } else if (c == ')') {
                counter--;
                if (counter == 0) {
                    end = i;
                    break;
                }
            }
        }

        if (start != -1 && end != -1) {
            String columnDef = query.substring(start + 1, end);
            String[] columns = columnDef.split(",");

            String newColDef = "";
            for (String col : columns) {
                if (!col.trim().equals("") && !newColDef.equals("")) {
                    newColDef += ",";
                }

                String arr[] = col.trim().split(" ", 2);

                if (!arr[0].equals(")")) {
                    newColDef += arr[0];
                }
            }

            return query.replace(query.substring(start + 1, end), newColDef);
        }

        return query;
    }
}
