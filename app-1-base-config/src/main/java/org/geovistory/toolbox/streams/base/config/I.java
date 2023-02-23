package org.geovistory.toolbox.streams.base.config;

public enum I {
    DEFAULT_PROJECT(375669),
    EN(18889),
    DE(18605);


    private final int value;

    I(int value) {
        this.value = value;
    }

    public int get() {
        return value;
    }
}
