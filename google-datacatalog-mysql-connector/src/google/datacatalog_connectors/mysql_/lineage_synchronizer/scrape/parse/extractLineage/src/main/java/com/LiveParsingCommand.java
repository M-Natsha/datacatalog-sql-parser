package com;

import java.util.Scanner;

public class LiveParsingCommand extends Command {
    @Override
    public void Run(String... args) throws Exception {
        Scanner in = new Scanner(System.in);
        in.useDelimiter(";");
        String data;

        do {
            // TODO: UPDATE THIS! I THINK ITS TOO BAD AND SWITCH IT TO listener
            // Wait Until getting a line
            data = in.next();

            // TODO: change this to a better command
            if (data.equalsIgnoreCase("exit")) {
                in.close();
                break;
            }

            String json = ParseSql.parseSqlToJson(data);
            System.out.println(json);

        } while (true);
    }
}
