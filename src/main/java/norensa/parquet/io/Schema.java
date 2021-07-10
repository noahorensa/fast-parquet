package norensa.parquet.io;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class Schema implements Iterable<ColumnIdentifier> {

    private int numCols;
    private List<ColumnIdentifier> columns;
    private HashMap<String, Integer> nameToIndexMap;

    Schema() {
        numCols = 0;
        columns = new ArrayList<>();
        nameToIndexMap = new HashMap<>();
    }

    public int getNumCols() {
        return numCols;
    }

    void add(ColumnIdentifier columnIdentifier) {
        columns.add(columnIdentifier);
        nameToIndexMap.put(columnIdentifier.getName(), numCols++);
    }

    public int getColumnIndex(String columnName) {
        return nameToIndexMap.get(columnName);
    }

    public ColumnIdentifier getColumn(int i) {
        return columns.get(i);
    }

    public ColumnIdentifier getColumn(String columnName) {
        return columns.get(nameToIndexMap.get(columnName));
    }

    public boolean isNullable(int i) {
        return columns.get(i).isNullable();
    }

    public boolean isNullable(String columnName) {
        return columns.get(nameToIndexMap.get(columnName)).isNullable();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof Schema) {
            Schema that = (Schema) obj;

            Iterator<ColumnIdentifier> it = that.columns.iterator();

            for (ColumnIdentifier i : this.columns) {
                if (i.equals(it.next()) == false) {
                    return false;
                }
            }

            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public Iterator<ColumnIdentifier> iterator() {
        return columns.iterator();
    }
}
