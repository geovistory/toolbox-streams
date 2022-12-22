package org.geovistory.toolbox.streams.utilities.cli;

import org.apache.commons.cli.*;

import java.io.PrintWriter;

public class CLI {

    public static final Option OPT_HELP = new Option("h", "help", false, "print help");
    public static final Option OPT_LIST = new Option("s", "schemas", false, "list schemas of schema registry");
    public static final Option OPT_DELETE = new Option("d", "delete", true, "delete schemas of schema registry");

    public static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(System.out);
        pw.println("Toolbox Streams CLI");
        pw.println();
        formatter.printUsage(pw, 100, "./cli.sh [options] argument1 argument2 ...");
        formatter.printOptions(pw, 100, options, 2, 5);
        pw.close();
    }

    public static void main(String[] args) {

        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption(OPT_HELP);
        options.addOption(OPT_LIST);
        options.addOption(OPT_DELETE);



        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            if (line.hasOption(OPT_LIST.getLongOpt())) {
                // print the value of ARG_LIST
                System.out.println(OPT_LIST.getArgName());
            } else {
                printHelp(options);
            }
        } catch (ParseException exp) {
            System.out.println("Unexpected exception:" + exp.getMessage());
        }
    }
}
