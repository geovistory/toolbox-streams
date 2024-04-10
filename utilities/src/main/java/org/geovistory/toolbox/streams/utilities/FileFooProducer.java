package org.geovistory.toolbox.streams.utilities;

import ts.projects.project.Value;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * Helper class to produce Foo messages
 */
public class FileFooProducer {

    public static void main(String[] args) throws IOException {
        new FileFooProducer().serializeToFile();
    }

    public void serializeToFile() throws IOException {
        // Serialize foo1, foo2 and foo3 to disk
        Value foo1 = Value.newBuilder().setNotes("Project 1").build();
        Value foo2 = Value.newBuilder().setNotes("Project 2").build();
        Value foo3 = Value.newBuilder().setNotes("Project 3").build();
        DatumWriter<Value> userDatumWriter = new SpecificDatumWriter<>(Value.class);
        DataFileWriter<Value> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        String pathname = "utilities/tmp/foo.avro";
        var file = new File(pathname);
        file.mkdirs();
        file.delete();
        dataFileWriter.create(foo1.getSchema(), file);
        dataFileWriter.append(foo1);
        dataFileWriter.append(foo2);
        dataFileWriter.append(foo3);
        dataFileWriter.close();
    }
}
