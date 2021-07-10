package norensa.parquet.io;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;

public class Table implements Iterable<Object[]> {

    private long id;
    private long numRows = 0;
    private int numCols = 0;
    private Schema schema;
    private TreeMap<Long, DataPage>[] pageMap;
    private List<DataPage>[] pageList;
    private String name;

    private static long tableCount = 0;
    private static synchronized long generateTableIdentifier() {
        return tableCount++;
    }

    private static HashMap<Long, Table> tableMap = new HashMap<>();
    private static void registerTable(long tableId, Table table) {
        tableMap.put(tableId, table);
    }

    public static Table getTableById(long id) {
        return tableMap.get(id);
    }

    Table() {
        id = generateTableIdentifier();
        registerTable(id, this);
        name = "parquet_table_" + id;
    }

    Table(long numRows, Schema schema, List<DataPage> dataPages) {
        this();

        this.numCols = schema.getNumCols();
        this.schema = schema;

        pageMap = new TreeMap[numCols];
        for (int i = 0; i < numCols; ++i) {
            pageMap[i] = new TreeMap<>();
        }

        pageList = new LinkedList[numCols];
        for (int i = 0; i < numCols; ++i) {
            pageList[i] = new LinkedList<>();
        }

        append(numRows, schema, dataPages);
    }

    boolean append(long numRows, Schema schema, List<DataPage> dataPages) {
        if (this.schema  == null) {
            this.numCols = schema.getNumCols();
            this.schema = schema;

            pageMap = new TreeMap[numCols];
            for (int i = 0; i < numCols; ++i) {
                pageMap[i] = new TreeMap<>();
            }

            pageList = new LinkedList[numCols];
            for (int i = 0; i < numCols; ++i) {
                pageList[i] = new LinkedList<>();
            }
        }

        if (this.schema.equals(schema)) {
            for (DataPage p : dataPages) {
                p.setFirstRowIndex(p.getFirstRowIndex() + this.numRows);

                pageMap[p.getColumnIndex()].put(p.getFirstRowIndex(), p);

                int insertIndex = 0;
                for (DataPage k : pageList[p.getColumnIndex()]) {
                    if (k.getFirstRowIndex() < p.getFirstRowIndex()) {
                        insertIndex++;
                    }
                    else {
                        break;
                    }
                }

                pageList[p.getColumnIndex()].add(insertIndex, p);
            }

            this.numRows += numRows;

            return true;
        }

        return false;
    }

    public long getId() {
        return id;
    }

    public long getNumRows() {
        return numRows;
    }

    public int getNumCols() {
        return numCols;
    }

    public Schema getSchema() {
        return schema;
    }

    List<DataPage>[] getPageList() {
        return pageList;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    Object[] getRow(long rowIndex) {
        Object[] res = new Object[numCols];

        for (int i = 0; i < numCols; ++i) {
            DataPage p = pageMap[i].floorEntry(rowIndex).getValue();
            res[i] = p.get((int)(rowIndex - p.getFirstRowIndex()));
        }

        return res;
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new TableIterator(this);
    }

    public void show(long numRows) {
        StringBuilder line = new StringBuilder();

        int i = 0;
        for (ColumnIdentifier c : schema) {
            if (i++ > 0) {
                line.append(",");
            }
            line.append(c.getName());
        }
        System.out.println(line);
        line.setLength(0);

        i = 0;
        for (Object[] row : this) {
            if  (++i > numRows) {
                break;
            }

            for (int j = 0; j < row.length; ++j) {
                if (j > 0) {
                    line.append(",");
                }
                line.append(row[j].toString());
            }

            System.out.println(line);
            line.setLength(0);
        }
    }

    public void show() {
        show(20);
    }

    public void exportToCsv(String path) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(path));

        StringBuilder line = new StringBuilder();

        int  k = 0;
        for (ColumnIdentifier i : schema) {
            if (k++ > 0) {
                line.append(",");
            }
            line.append(i.getName());
        }
        writer.write(line.toString());
        writer.newLine();
        line.setLength(0);

        for (Object[] row : this) {
            for (int j = 0; j < row.length; ++j) {
                if (j > 0) {
                    line.append(",");
                }
                line.append(row[j].toString());
            }

            writer.write(line.toString());
            writer.newLine();
            line.setLength(0);
        }

        writer.close();
    }
}

