package org.geovistory.toolbox.streams.project.entity.preview;

public enum Klass {
    PERSISTENT_ITEM(70);


    private final int value;

    Klass(int value) {
        this.value = value;
    }

    public int get() {
        return value;
    }
}
