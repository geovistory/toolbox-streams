package org.geovistory.toolbox.streams.testlib;

import java.io.File;

public class FileRemover {

    public static void removeDir(File file) {
        if (!file.exists()) {
            System.out.println("File or directory does not exist.");
            return;
        }

        if (file.isDirectory()) {
            // Delete all files and subdirectories within the directory recursively
            File[] files = file.listFiles();
            if (files != null) {
                for (File subFile : files) {
                    removeDir(subFile);
                }
            }
        }

        // Delete the file or empty directory
        boolean success = file.delete();
        if (success) {
            System.out.println("File or directory deleted successfully: " + file.getAbsolutePath());
        } else {
            System.err.println("Failed to delete file or directory: " + file.getAbsolutePath());
        }
    }

    public static void removeDir(String pathToDirectory) {
        // Example usage: delete a directory and all its contents
        File directory = new File(pathToDirectory);
        removeDir(directory);
    }
}
