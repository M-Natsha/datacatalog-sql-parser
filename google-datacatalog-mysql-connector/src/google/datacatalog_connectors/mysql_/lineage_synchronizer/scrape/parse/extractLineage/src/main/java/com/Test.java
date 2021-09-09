package com;

public class Test {
    public static void main(String[] args) throws Exception {
        // Default Command
        Command command = new LiveParsingCommand();
        if(args.length > 0) {
            switch (args[0]) {
                case "-p":
                    command = new ParseCommand();
                    break;
            }
        }

        command.Run(args);
    }
}
