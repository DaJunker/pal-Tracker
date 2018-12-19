package io.pivotal.pal.tracker;

import org.springframework.stereotype.Component;

import java.util.*;

public class InMemoryTimeEntryRepository implements TimeEntryRepository {

    private Map<Long, TimeEntry> timeEntries = new HashMap<>();

    public TimeEntry create(TimeEntry timeEntry) {
        timeEntry.setTimeEntryId(timeEntries.keySet().stream().max(Comparator.naturalOrder()).orElse(0L) + 1);
        timeEntries.put(timeEntry.getId(), timeEntry);
        return timeEntry;
    }

    public TimeEntry find(long id) {
        return timeEntries.get(id);
    }

    public List<TimeEntry> list() {
        return new ArrayList<>(timeEntries.values());
    }

    public TimeEntry update(long id, TimeEntry timeEntry) {
        timeEntry.setTimeEntryId(id);
        timeEntries.put(id, timeEntry);
        return timeEntry;
    }

    public void delete(long id) {
        timeEntries.remove(id);
    }
}
