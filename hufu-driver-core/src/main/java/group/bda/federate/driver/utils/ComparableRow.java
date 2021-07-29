package group.bda.federate.driver.utils;

import group.bda.federate.data.Row;

public class ComparableRow<K extends Comparable<K>> implements Comparable<ComparableRow<K>> {
    public K compareKey;
    public Row row;

    public ComparableRow(K compareKey, Row row) {
        this.compareKey = compareKey;
        this.row = row;
    }

    @Override
    public int compareTo(ComparableRow<K> o) {
        return compareKey.compareTo(o.compareKey); // need to be ascending
    }
}