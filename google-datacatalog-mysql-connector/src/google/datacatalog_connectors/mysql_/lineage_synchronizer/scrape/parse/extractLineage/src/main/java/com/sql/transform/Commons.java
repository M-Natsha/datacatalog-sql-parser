package com.sql.transform;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Commons {
    private static boolean balancedString(String text) {
        int count = (int) text.chars().filter(num -> num == '\"').count();
        if(count % 2 == 1) {
            return false;
        }

        count = (int) text.chars().filter(num -> num == '\'').count();
        if(count % 2 == 1) {
            return false;
        }

        Stack<String> stack = new Stack<>();

        for(int i=0; i < text.length();i++) {
            String ch = text.charAt(i) + "";
            if(ch.equals("(") || ch.equals('[')) {
                stack.push(ch);
            } else if( ch.equals(")")) {
                if(!stack.empty() && stack.peek().equals("(")) {
                    stack.pop();
                } else {
                    return false;
                }
            } else if(ch.equals("]")) {
                if(!stack.empty() && stack.peek().equals("[")) {
                    stack.pop();
                } else {
                    return false;
                }
            }
        }

        if(!stack.empty()) {
            return false;
        }

        return true;
    }

        public static int findSqlKeyword(String text, String target) {
        Matcher m;
        m = Pattern.compile(target, Pattern.CASE_INSENSITIVE).matcher(text);
        List<Integer> posList = new ArrayList<Integer>();
        while (m.find())
        {
            posList.add(m.start());
        }

        for(int pos: posList) {
            String beforeFrom = text.substring(0,pos);
            if(balancedString(beforeFrom)) {
                return pos;
            }
        }

        return -1;
    }
}
