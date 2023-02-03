package org.geovistory.toolbox.streams.utils;

import org.geovistory.toolbox.streams.avro.ProjectStatementValue;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.List;

public class TopStatementAdder {
    public static List<ProjectStatementValue> addStatement(List<ProjectStatementValue> existingList, ProjectStatementValue newItem, boolean isOutgoing) {
        int targetPosition = -1;
        int replacedItemPosition = -1;

        for (int i = 0; i < existingList.size(); i++) {
            var oldItem = existingList.get(i);
            var oldOrdNum = isOutgoing ? oldItem.getOrdNumOfRange() : oldItem.getOrdNumOfDomain();
            var newOrdNum = isOutgoing ? newItem.getOrdNumOfRange() : newItem.getOrdNumOfDomain();
            var oldId = oldItem.getStatementId();
            var newId = newItem.getStatementId();
            // if newOrdNum <= oldOrdNum ...
            if (newOrdNum != null && oldOrdNum != null && newOrdNum <= oldOrdNum && targetPosition == -1) {
                // ...set the target position of the new item
                targetPosition = i;
            }
            // if both items have no explicit ordNum...
            if (newOrdNum == null && oldOrdNum == null && targetPosition == -1) {

                var oldModifiedAt = Utils.DateFromIso(oldItem.getModifiedAt());
                var newModifiedAt = Utils.DateFromIso(newItem.getModifiedAt());

                // if both have a date
                if (oldModifiedAt != null && newModifiedAt != null) {
                    // ... and the new item has a more recent modification date...
                    if (newModifiedAt.after(oldModifiedAt)) {
                        targetPosition = i;
                    } else if (newModifiedAt.equals(oldModifiedAt)) {
                        // order by id
                        if (newId > oldId) targetPosition = i;
                    }
                    // else do nothing -> the new item will not be positioned before the old
                }
                // if newItem has a date (and oldItem not, given by above if clause)
                else if (newModifiedAt != null) {
                    // ...set the target position of the new item
                    targetPosition = i;
                }
                // if both have no date (newModifiedAt is always null at this point)
                else if (oldModifiedAt == null) {
                    // order by id
                    if (newId > oldId) targetPosition = i;
                }
                // else do nothing -> the new item will not be positioned before the old

            }
            // if the newItem has an ordNum, the old doesn't, set position
            if (newOrdNum != null && oldOrdNum == null && targetPosition == -1) {
                targetPosition = i;
            }
            // if the newItem replaces an oldItem...
            if (oldId == newId) {
                // ...keep track of this old item
                replacedItemPosition = i;
            }
        }

        // if item was deleted...
        if (newItem.getDeleted$1() != null && newItem.getDeleted$1()) {
            // ...remove it
            if (replacedItemPosition > -1) existingList.remove(replacedItemPosition);
            // ...and return immediately
            return existingList;
        }

        // add item at retrieved position
        if (targetPosition > -1) {
            existingList.add(targetPosition, newItem);
            // if it was inserted before the replaced item...
            if (targetPosition < replacedItemPosition) {
                // ...increase replace item position
                replacedItemPosition = replacedItemPosition + 1;
            }
        }
        // append item to the end
        else if (existingList.size() < 5) {
            existingList.add(newItem);
        }

        // remove the replaced item
        if (replacedItemPosition > -1) {
            existingList.remove(replacedItemPosition);
        }

        // keep max. list size
        if (existingList.size() > 5) existingList.remove(5);

        return existingList;
    }

}
