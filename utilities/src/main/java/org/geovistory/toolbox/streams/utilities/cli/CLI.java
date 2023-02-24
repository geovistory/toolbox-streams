package org.geovistory.toolbox.streams.utilities.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.geovistory.toolbox.streams.lib.AppConfig;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class CLI {

    public static final Option OPT_PRINT_SCHEMA_REGISTRY_URL = new Option("u", "print-url", false, "print schema registry url");
    public static final Option OPT_HELP = new Option("h", "help", false, "print help");
    public static final Option OPT_LIST_SCHEMAS = new Option("s", "list-schemas", false, "list schemas of schema registry");
    public static final Option OPT_DELETE_SCHEMAS = new Option("d", "delete-schemas", true, "soft-delete schemas of schema registry containing the provided <arg> string. This will not delete unless you provide the --confirm flag.");
    public static final Option OPT_LIST_TOPICS = new Option(null, "list-topics", false, "list topics");
    public static final Option OPT_LIST_TOPIC_CONFIGS = new Option(null, "list-topic-configs", false, "list topic configs");
    public static final Option OPT_DELETE_TOPICS = new Option(null, "delete-topics", true, "delete topics containing the provided <arg> string. This will not delete unless you provide the --confirm flag.");
    public static final Option OPT_CONFIRM = new Option(null, "confirm", false, "Confirm performing a dangerous opteration.");
    public static final Option OPT_DELETE_SCHEMAS_AND_TOPICS = new Option("d", "delete-schemas-and-topics", true, "executes --delete-topics and --delete-schemas");

    public static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(System.out);
        pw.println("Toolbox Streams CLI");
        pw.println();
        formatter.printUsage(pw, 100, "./gradlew utilities:cli --args=\"--argument1=foo --argument2 ...\"");
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
        options.addOption(OPT_LIST_SCHEMAS);
        options.addOption(OPT_DELETE_SCHEMAS);
        options.addOption(OPT_LIST_TOPICS);
        options.addOption(OPT_DELETE_TOPICS);
        options.addOption(OPT_CONFIRM);
        options.addOption(OPT_LIST_TOPIC_CONFIGS);
        options.addOption(OPT_DELETE_SCHEMAS_AND_TOPICS);

        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);
            if (line.hasOption(OPT_PRINT_SCHEMA_REGISTRY_URL.getLongOpt())) {
                printSchemaRegistryURL();
            } else if (line.hasOption(OPT_LIST_SCHEMAS.getLongOpt())) {
                var schemas = getSchemas();
                schemas.forEach(System.out::println);
                System.out.println(schemas.size() + " schemas found.");
            } else if (line.hasOption(OPT_DELETE_SCHEMAS.getLongOpt())) {
                Boolean confirmed = line.hasOption(OPT_CONFIRM.getLongOpt());
                deleteSchemas(line.getOptionValue(OPT_DELETE_SCHEMAS.getLongOpt()), confirmed);
            } else if (line.hasOption(OPT_LIST_TOPICS.getLongOpt())) {
                var topics = getTopics();
                topics.forEach(System.out::println);
                System.out.println(topics.size() + " topics found.");

            } else if (line.hasOption(OPT_DELETE_TOPICS.getLongOpt())) {
                Boolean confirmed = line.hasOption(OPT_CONFIRM.getLongOpt());
                deleteTopics(line.getOptionValue(OPT_DELETE_TOPICS.getLongOpt()), confirmed);

            } else if (line.hasOption(OPT_DELETE_SCHEMAS_AND_TOPICS.getLongOpt())) {
                Boolean confirmed = line.hasOption(OPT_CONFIRM.getLongOpt());
                deleteSchemas(line.getOptionValue(OPT_DELETE_SCHEMAS_AND_TOPICS.getLongOpt()), confirmed);
                deleteTopics(line.getOptionValue(OPT_DELETE_SCHEMAS_AND_TOPICS.getLongOpt()), confirmed);
            } else if (line.hasOption(OPT_LIST_TOPIC_CONFIGS.getLongOpt())) {
                var configs = getTopicConfigs();
                configs.forEach(System.out::println);
                System.out.println(configs.size() + " topic configs listed.");

            } else {
                printHelp(options);
            }
        } catch (ParseException |
                 IOException exp) {
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
        assert response.body() != null;
        var s = response.body().string();
        ObjectMapper objectMapper = new ObjectMapper();
        StringArrayList schemas = objectMapper.readValue(s, StringArrayList.class);
        response.close();
        return schemas;
    }

    public static List<String> getTopics() {
        AdminClient adminClient = getAdminClient();
        var topicList = adminClient.listTopics();
        try {
            return topicList.names().get().stream().toList();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> getTopicConfigs() {
        AdminClient adminClient = getAdminClient();
        var x = getTopics().stream().map(s -> new ConfigResource(ConfigResource.Type.TOPIC, s)).toList();
        var res = adminClient.describeConfigs(x);

        try {
            var v = res.all().get();
            return v.entrySet().stream().map(config -> config.getKey()
                    .name() + "\n" +
                    "cleanup.policy:" + config.getValue().get("cleanup.policy").value() + "\n"
            ).toList();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }


    }

    private static AdminClient getAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.INSTANCE.getKafkaBootstrapServers());
        return AdminClient.create(props);
    }

    public static void deleteTopics(String prefix, Boolean confirmed) {
        AdminClient adminClient = getAdminClient();
        var topics = getTopics();
        var filtered = topics.stream().filter(s -> s.contains(prefix)).toList();
        filtered.forEach(System.out::println);
        if (confirmed) {
            var res = adminClient.deleteTopics(filtered);
            try {
                res.all().get();
                System.out.println("Successfully deleted: " + filtered.size());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

        } else {
            System.out.println("To delete the above " + filtered.size() + " topics add the flag --confirm");
        }
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
