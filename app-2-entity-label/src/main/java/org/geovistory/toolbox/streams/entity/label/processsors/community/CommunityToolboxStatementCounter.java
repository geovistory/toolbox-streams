package org.geovistory.toolbox.streams.entity.label.processsors.community;

import org.geovistory.toolbox.streams.avro.ProjectStatementValue;
import org.geovistory.toolbox.streams.lib.Utils;

public class CommunityToolboxStatementCounter extends CommunityStatementCounter {


    /**
     * @param stateStoreName name of state store
     */
    public CommunityToolboxStatementCounter(String stateStoreName) {
        super(stateStoreName);
    }

    /**
     * Validator function for project statements. Checks visibility of subject and object, and the __deleted flag.
     *
     * @return true, if the statement is visible for the toolbox community, else false
     */
    @Override
    protected boolean isAccepted(ProjectStatementValue projectStatementValue) {
        var stmt = projectStatementValue.getStatement();
        var s = stmt.getSubject();
        var o = stmt.getObject();

        // return false, if record was deleted
        if (Utils.booleanIsEqualTrue(projectStatementValue.getDeleted$1())) return false;

        // return false, if subject is not entity
        if (s == null || s.getEntity() == null) return false;

        // return false, if subject is not visible for the toolbox community
        if (!s.getEntity().getCommunityVisibilityToolbox()) return false;

        // return false, if object is null
        if (o == null) return false;

        // return true, if object is a literal or the toolbox visibility of the entity
        return o.getEntity() == null || o.getEntity().getCommunityVisibilityToolbox();
    }
}