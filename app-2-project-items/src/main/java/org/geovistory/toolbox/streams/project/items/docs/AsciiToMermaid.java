package org.geovistory.toolbox.streams.project.items.docs;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.geovistory.toolbox.streams.project.items.docs.AsciiToMermaid.AsciiToMermaidUtil.nameFunction;


public class AsciiToMermaid {

    public static String toMermaid(String topology) {
        String[] lines = topology.split("\n");
        List<String> subTopologies = new ArrayList<>();
        List<String> outside = new ArrayList<>();
        CurrentGraphNodeNameRef currentGraphNodeName = new CurrentGraphNodeNameRef("");
        List<String> subTopologiesList = new ArrayList<>();
        List<String> topicSourcesList = new ArrayList<>();
        List<String> topicSinksList = new ArrayList<>();
        List<String> stateStoresList = new ArrayList<>();

        for (String line : lines) {
            if (SubTopology.pattern.matcher(line).find()) {
                SubTopology.visit(line, subTopologies, subTopologiesList);
            } else if (Source.pattern.matcher(line).find()) {
                Source.visit(line, outside, topicSourcesList, currentGraphNodeName);
            } else if (Processor.pattern.matcher(line).find()) {
                Processor.visit(line, currentGraphNodeName, outside, stateStoresList);
            } else if (Sink.pattern.matcher(line).find()) {
                Sink.visit(line, currentGraphNodeName, outside, topicSinksList);
            } else if (RightArrow.pattern.matcher(line).find()) {
                RightArrow.visit(line, currentGraphNodeName, subTopologies);
            }
        }

        // Move the first item to the end
        if (subTopologies.size() > 1) {
            String firstItem = subTopologies.remove(0);
            subTopologies.add(firstItem);
        }

        List<String> result = new ArrayList<>();
        result.add("graph TD");
        result.addAll(outside);
        result.addAll(subTopologies);
        result.addAll(topicSourcesList);
        result.addAll(topicSinksList);
        result.addAll(stateStoresList);

        return String.join("\n", result);
    }


    static class SubTopology {
        public static Pattern pattern = Pattern.compile("Sub-topology: ([0-9]*)");

        public static String startFormatter(String subTopology) {
            return "subgraph Sub-Topology: " + subTopology;
        }

        public static String endFormatter() {
            return "end";
        }

        public static void visit(String line, List<String> subTopologies, List<String> subTopologiesList) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                String subTopology = matcher.group(1);
                subTopologies.add(endFormatter());
                subTopologies.add(startFormatter(subTopology));
                subTopologiesList.add(subTopology);
            }
        }
    }

    static class Source {
        public static Pattern pattern = Pattern.compile("Source:\\s+(\\S+)\\s+\\(topics:\\s+\\[(.*)\\]\\)");

        public static String formatter(String source, String topic) {
            return topic + "[" + topic + "] --> " + source + "(" + nameFunction(source) + ")";
        }

        public static void visit(String line, List<String> outside, List<String> topicSourcesList, CurrentGraphNodeNameRef ref) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                ref.currentGraphNodeName = matcher.group(1).trim();
                String topics = matcher.group(2);
                for (String topic : topics.split(",")) {
                    topic = topic.trim();
                    outside.add(formatter(ref.getCurrentGraphNodeName(), topic));
                    topicSourcesList.add(topic);
                }
            }
        }
    }

    static class Processor {
        public static Pattern pattern = Pattern.compile("Processor:\\s+(\\S+)\\s+\\(stores:\\s+\\[(.*)\\]\\)");

        public static String formatter(String processor, String store) {
            if (processor.contains("JOIN")) {
                return store + "[(" + nameFunction(store) + ")] --> " + processor + "(" + nameFunction(processor) + ")";
            } else {
                return processor + "(" + nameFunction(processor) + ") --> " + store + "[(" + nameFunction(store) + ")]";
            }
        }

        public static void visit(String line, CurrentGraphNodeNameRef ref, List<String> outside, List<String> stateStoresList) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                ref.currentGraphNodeName = matcher.group(1).trim();
                String stores = matcher.group(2);
                for (String store : stores.split(",")) {
                    store = store.trim();
                    if (stores.length() > 0) {
                        outside.add(formatter(ref.currentGraphNodeName, store));
                        stateStoresList.add(store);
                    }
                }
            }
        }
    }

    static class Sink {
        public static Pattern pattern = Pattern.compile("Sink:\\s+(\\S+)\\s+\\(topic:\\s+(.*)\\)");

        public static String formatter(String sink, String topic) {
            return sink + "(" + nameFunction(sink) + ") --> " + topic + "[" + topic + "]";
        }

        public static void visit(String line, CurrentGraphNodeNameRef ref, List<String> outside, List<String> topicSinksList) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                ref.currentGraphNodeName = matcher.group(1).trim();
                String topic = matcher.group(2).trim();
                outside.add(formatter(ref.currentGraphNodeName, topic));
                topicSinksList.add(topic);
            }
        }
    }

    static class RightArrow {
        public static Pattern pattern = Pattern.compile("\\s*-->\\s+(.*)");

        public static String formatter(String src, String dst) {
            return src + "(" + nameFunction(src) + ") --> " + dst + "(" + nameFunction(dst) + ")";
        }

        public static void visit(String line, CurrentGraphNodeNameRef ref, List<String> subTopologies) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                for (String target : matcher.group(1).split(",")) {
                    target = target.trim();
                    if (!target.equals("none")) {
                        subTopologies.add(formatter(ref.getCurrentGraphNodeName(), target));
                    }
                }
            }
        }
    }

    // Define nameFunction method
    static class AsciiToMermaidUtil {
        public static String nameFunction(String value) {
            return value.replaceAll("-", "-<br>");
        }
    }

    static class CurrentGraphNodeNameRef {
        private String currentGraphNodeName;

        public CurrentGraphNodeNameRef(String currentGraphNodeName) {
            this.currentGraphNodeName = currentGraphNodeName;
        }

        public String getCurrentGraphNodeName() {
            return currentGraphNodeName;
        }

        public void setCurrentGraphNodeName(String currentGraphNodeName) {
            this.currentGraphNodeName = currentGraphNodeName;
        }
    }

}
