package org.geovistory.toolbox.streams.utilities;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class StringSanitizerTest {

    @Test
    void escapeBackslash() {
        var input = "\\";
        var output = StringSanitizer.escapeBackslashAndDoubleQuote(input);
        assertThat(output).isEqualTo("\\\\");
    }
    @Test
    void escapeNewlineBackslash() {
        var input = "\\n\\";
        var output = StringSanitizer.escapeBackslashAndDoubleQuote(input);
        assertThat(output).isEqualTo("\\\\n\\\\");
    }
}
