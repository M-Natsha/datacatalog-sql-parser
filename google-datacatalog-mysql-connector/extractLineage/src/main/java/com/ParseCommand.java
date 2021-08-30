package com;

public class ParseCommand extends Command{
    @Override
    public void Run(String... args) throws Exception {
        String query = args[1];
        String json = ParseSql.parseSqlToJson(query);
        System.out.println(json);
    }
}
