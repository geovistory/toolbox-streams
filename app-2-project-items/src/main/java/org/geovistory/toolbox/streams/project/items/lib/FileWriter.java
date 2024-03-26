package org.geovistory.toolbox.streams.project.items.lib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileWriter {
    private static final Logger LOG = LoggerFactory.getLogger(FileWriter.class);

    /**
     * Writes the specified text content to the file specified by the given file path.
     *
     * @param filePath The path to the file where the text will be written.
     * @param text     The text content to write to the file.
     */
    public static void write(String filePath, String text) {


        // Write text to the file
        try {
            // Convert text to bytes
            byte[] bytes = text.getBytes();

            // Create a Path object
            Path path = Paths.get(filePath);

            // Write bytes to the file
            Files.write(path, bytes);

            LOG.info("Text has been written to " + filePath);
        } catch (IOException | IllegalArgumentException e) {
            LOG.error("Failed to write to the file: " + e.getMessage());
        }
    }
}
