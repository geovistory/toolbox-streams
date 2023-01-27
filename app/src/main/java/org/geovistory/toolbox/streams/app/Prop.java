package org.geovistory.toolbox.streams.app;

public enum Prop {
    HAS_TYPE(2);


    private final int value;

    Prop(int value) {
        this.value = value;
    }

    public int get() {
        return value;
    }
}
