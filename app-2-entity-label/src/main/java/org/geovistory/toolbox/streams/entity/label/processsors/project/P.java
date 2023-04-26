package org.geovistory.toolbox.streams.entity.label.processsors.project;

/**
 * Processor names
 */
public enum P {

    // pro_statement
    PRO_STMT_SOURCE,
    PRO_STMT_REPARTITION_BY_SUBJECT,
    PRO_STMT_BY_SUBJECT_SINK,
    PRO_STMT_BY_SUBJECT_SOURCE,
    PRO_STMT_BY_SUBJECT_PROCESSOR,

    PRO_STMT_REPARTITION_BY_OBJECT,
    PRO_STMT_BY_OBJECT_SINK,
    PRO_STMT_BY_OBJECT_SOURCE,
    PRO_STMT_BY_OBJECT_PROCESSOR,

    // community_tlbx_label
    COM_TLBX_LABEL_SOURCE(), // might be used in future (when CommunityTopIn/OutStatements is converted to Processor API)
    COM_TLBX_LABEL_REPARTITION_BY_ID,
    COM_TLBX_LABEL_BY_ID_SINK,
    COM_TLBX_LABEL_BY_ID_SOURCE,
    COM_TLBX_LABEL_BY_ID_PROCESSOR,

    // pro_label
    PRO_LABEL_SOURCE,
    PRO_LABEL_REPARTITION_BY_ID,
    PRO_LABEL_BY_ID_SINK,
    PRO_LABEL_BY_ID_SOURCE,
    PRO_LABEL_BY_ID_PROCESSOR,

    // pro_in_statement_join
    PRO_INC_STATEMENT_LABEL_JOINER,
    PRO_INC_STATEMENT_LABEL_SINK,
    PRO_INC_STATEMENT_LABEL_SOURCE,


    // pro_out_statement_join
    PRO_OUT_STATEMENT_LABEL_JOINER,
    PRO_OUT_STATEMENT_LABEL_SINK,
    PRO_OUT_STATEMENT_LABEL_SOURCE,

    // pro_top_in_statement
    PRO_TOP_INC_STATEMENT_AGGREGATOR,
    PRO_TOP_INC_STATEMENT_SINK,

    // pro_top_out_statement
    PRO_TOP_OUT_STATEMENT_AGGREGATOR,
    PRO_TOP_OUT_STATEMENT_SINK,
    ;

    /**
     * get string
     * @return string value
     */
    public String s() {
        return toString().toLowerCase().replace("_", "-");
    }

}