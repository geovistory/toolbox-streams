package org.geovistory.toolbox.streams.utilities;

import dev.projects.project.Value;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;

/**
 * Helper class to produce Foo messages
 */
public class FileFooConsumer {

    public static void main(String[] args) throws IOException {
        new FileFooConsumer().deserializeFromFile();
    }

    public void deserializeFromFile() throws IOException {
        DatumReader<Value> userDatumReader = new SpecificDatumReader<>(Value.class);
        File file = new File("utilities/tmp/foo.avro");
        DataFileReader<Value> dataFileReader = new DataFileReader<>(file, userDatumReader);
        Value foo = null;
        while (dataFileReader.hasNext()) {
            // Reuse user object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            foo = dataFileReader.next(foo);
            System.out.println(foo);
        }
    }
}
