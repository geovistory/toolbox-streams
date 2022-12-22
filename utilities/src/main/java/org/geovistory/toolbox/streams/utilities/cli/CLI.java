package org.geovistory.toolbox.streams.utilities.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.cli.*;
import org.geovistory.toolbox.streams.lib.AppConfig;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class CLI {

    public static final Option OPT_PRINT_SCHEMA_REGISTRY_URL = new Option("u", "print-url", false, "print schema registry url");
    public static final Option OPT_HELP = new Option("h", "help", false, "print help");
    public static final Option OPT_LIST = new Option("s", "list-schemas", false, "list schemas of schema registry");
    public static final Option OPT_DELETE = new Option("d", "delete-schemas", true, "soft-delete schemas of schema registry containing the provided <arg> string. This will not delete unless you provide the --confirm flag.");
    public static final Option OPT_CONFIRM = new Option(null, "confirm", false, "Confirm performing a dangerous opteration.");

    public static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(System.out);
        pw.println("Toolbox Streams CLI");
        pw.println();
        formatter.printUsage(pw, 100, "./cli.sh [options] argument1 argument2 ...");
        formatter.printOptions(pw, 100, options, 2, 5);
        pw.close();
    }

    public static void main(String[] args) throws IOException {
        getSchemas();
        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption(OPT_PRINT_SCHEMA_REGISTRY_URL);
        options.addOption(OPT_HELP);
        options.addOption(OPT_LIST);
        options.addOption(OPT_DELETE);
        options.addOption(OPT_CONFIRM);


        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);
            if (line.hasOption(OPT_PRINT_SCHEMA_REGISTRY_URL.getLongOpt())) {
                printSchemaRegistryURL();
            } else if (line.hasOption(OPT_LIST.getLongOpt())) {
                var schemas = getSchemas();
                schemas.forEach(System.out::println);
                System.out.println(schemas.size() + " schemas found.");
            } else if (line.hasOption(OPT_DELETE.getLongOpt())) {
                Boolean confirmed = line.hasOption(OPT_CONFIRM.getLongOpt());
                deleteSchemas(line.getOptionValue(OPT_DELETE.getLongOpt()), confirmed);
            } else {
                printHelp(options);
            }
        } catch (ParseException | IOException exp) {
            System.out.println("Unexpected exception:" + exp.getMessage());
        }
    }

    public static void printSchemaRegistryURL() {
        var url = AppConfig.INSTANCE.getSchemaRegistryUrl();
        System.out.println("Schema Registry URL");
        System.out.println(url);
    }

    public static StringArrayList getSchemas() throws IOException {
        OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        Request request = new Request.Builder()
                .url(AppConfig.INSTANCE.getSchemaRegistryUrl() + "/subjects")
                .method("GET", null)
                .build();
        Response response = client.newCall(request).execute();
        var s = response.body().string();
        ObjectMapper objectMapper = new ObjectMapper();
        StringArrayList schemas = objectMapper.readValue(s, StringArrayList.class);
        response.close();
        return schemas;
    }

    public static void deleteSchemas(String prefix, Boolean confirmed) throws IOException {
        var schemas = getSchemas();
        var filtered = schemas.stream().filter(s -> s.contains(prefix)).toList();
        filtered.forEach(System.out::println);
        if (confirmed) {
            AtomicInteger successfulDeletes = new AtomicInteger();
            AtomicInteger errors = new AtomicInteger();
            filtered.forEach(schemaName -> {
                OkHttpClient client = new OkHttpClient().newBuilder()
                        .build();
                Request request = new Request.Builder()
                        .url(AppConfig.INSTANCE.getSchemaRegistryUrl() + "/subjects/" + schemaName)
                        .method("DELETE", null)
                        .build();
                Response response;
                try {
                    response = client.newCall(request).execute();
                    if (response.code() == 200) {
                        System.out.println("Deleted " + schemaName);
                        successfulDeletes.getAndIncrement();
                    }
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                    errors.getAndIncrement();
                }
            });

            System.out.println("Successfully deleted: " + successfulDeletes + " Errors: " + errors);
        } else {
            System.out.println("To delete the above " + filtered.size() + " schemas add the flag --confirm");
        }


    }

    public static class StringArrayList extends ArrayList<String> {
    }

}
