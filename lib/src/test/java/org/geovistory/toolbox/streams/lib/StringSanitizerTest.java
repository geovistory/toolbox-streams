package org.geovistory.toolbox.streams.lib;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class StringSanitizerTest {

    @Test
    void escapeBackslash() {
        var input = "\\";
        var output = StringSanitizer.escapeJava(input);
        assertThat(output).isEqualTo("\\\\");
    }

    @Test
    void escapeNewlineBackslash() {
        var input = "\\n\\";
        var output = StringSanitizer.escapeJava(input);
        assertThat(output).isEqualTo("\\\\n\\\\");
    }

    @Test
    void escapeNewline() {
        var input = "foo \n";
        var output = StringSanitizer.escapeJava(input);
        assertThat(output).isEqualTo("foo \\n");
    }

    @Test
    void escapeAll() {
        var input = "This is a string with special characters: \b \f \' \r \t \n \" ' \\";
        var output = StringSanitizer.escapeJava(input);
        assertThat(output).isEqualTo("This is a string with special characters: \\b \\f ' \\r \\t \\n \\\" ' \\\\");
    }


}
