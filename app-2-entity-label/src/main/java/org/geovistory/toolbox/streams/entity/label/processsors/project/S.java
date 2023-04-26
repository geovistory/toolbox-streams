package org.geovistory.toolbox.streams.entity.label.processsors.project;

/**
 * Store names
 */
public enum S {
    PRO_STMT_BY_SUBJECT_STORE,
    PRO_INC_STMT_LABEL_JOIN_STORE,
    PRO_INC_STMT_WITHOUT_PROJECT_LABEL_STORE,
    PRO_TOP_INC_STMT_STORE,

    PRO_STMT_BY_OBJECT_STORE,
    PRO_OUT_STMT_LABEL_JOIN_STORE,
    PRO_OUT_STMT_WITHOUT_PROJECT_LABEL_STORE,
    PRO_TOP_OUT_STMT_STORE,

    COM_TLBX_LABEL_STORE,
    PRO_LABEL_STORE;


    String v() {
        return toString().toLowerCase().replace("_", "-");
    }

}


