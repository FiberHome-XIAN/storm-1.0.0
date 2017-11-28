package org.apache.storm.daemon;

import clojure.lang.Keyword;
import com.google.common.collect.*;
import org.apache.storm.generated.ExecutorInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class LaunchAttemptCounter {

    private static final Logger LOGGER = LoggerFactory.getLogger(LaunchAttemptCounter.class);

    private static final HashBasedTable<String, Collection<ExecutorInfo>, Integer> table = HashBasedTable.create();

    public static int getAttemptCount(String id, Collection<ExecutorInfo> executors) {
//        LOGGER.info(id + "  :  " +executors+ "-----------getAttemptCount-attempt-table------------->" + table);
        return id != null && executors != null && table.contains(id, executors) ? table.get(id, executors) : 0;
    }

    public synchronized static int getAttemptCountAndInc(String id, Collection<ExecutorInfo> executors) {
        if (id == null || executors == null) {
            return 0;
        }
        Integer count = table.get(id, executors);
        if (count == null) {
            count = 0;
        }
        table.put(id, executors, ++count);
//        LOGGER.info(id + "  :  " +executors+ "-----------getAttemptCountAndInc-attempt-table------------->" + table);
        return count;
    }

    public synchronized static void clearAttempts(Map<Integer, Map<Keyword, Object>> allAssignments) {
        if (allAssignments == null || table.isEmpty()) {
            return;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("-------clearAttempts------->assignments:{}; attemptTable:{}", allAssignments, table);
        }
        if (allAssignments.isEmpty()) {
            LOGGER.info("-------clearAttempt------->no assignment exists, clear attempt table.");
            table.clear();
            return;
        }
        Multimap<String, Collection<ExecutorInfo>> assignments = HashMultimap.create();
        Multimap<String, Collection<ExecutorInfo>> deleteAttempts = HashMultimap.create();
        for (Map<Keyword, Object> assignment : allAssignments.values()) {
            assignments.put((String) assignment.get(Keyword.find("storm-id")), (Collection<ExecutorInfo>) assignment.get(Keyword.find("executors")));
        }
        if (assignments.isEmpty()) {
            return;
        }
        for (Table.Cell<String, Collection<ExecutorInfo>, Integer> cell : table.cellSet()) {
            if (!assignments.containsEntry(cell.getRowKey(), cell.getColumnKey())) {
                // get the not exist assignment attempt counter
                deleteAttempts.put(cell.getRowKey(), cell.getColumnKey());
            }
        }
        // remove not exist assignment attempt counter
        for (Map.Entry<String, Collection<ExecutorInfo>> entry : deleteAttempts.entries()) {
            String id = entry.getKey();
            Collection<ExecutorInfo> executors = entry.getValue();
            LOGGER.info("-------clearAttempt------->storm-id: {}, executors: {}, attempt-count: {}", id, executors, table.get(id, executors));
            table.remove(id, executors);
        }

    }
}
