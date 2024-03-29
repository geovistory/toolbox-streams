import org.gradle.api.DefaultTask;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

public abstract class VersionInfo extends DefaultTask {

    @Input
    public abstract Property<String> getImageName();

    @OutputFile
    public abstract RegularFileProperty getOutputFile();

    @TaskAction
    public void create() throws IOException {
        Properties prop = new Properties();
        prop.setProperty("imageName", getImageName().get());
        try (OutputStream output = new FileOutputStream(getOutputFile().getAsFile().get())) {
            prop.store(output, null);
        }
    }
}
