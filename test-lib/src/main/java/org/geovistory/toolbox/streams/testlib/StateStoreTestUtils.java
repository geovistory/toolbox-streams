package org.geovistory.toolbox.streams.testlib;

import java.util.Iterator;

public class StateStoreTestUtils {
    public static <E> int countItems(Iterator<E> iterator) {
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

}
