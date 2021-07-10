package norensa.parquet.spark;

import norensa.parquet.io.Table;
import norensa.parquet.io.SparkTablePartitionIterator;
import org.apache.spark.Partition;

public class TablePartition implements Partition {

    private int index;
    private long offset;
    private long limit;
    private long tableId;

    TablePartition(int index, int numPartitions, Table table) {
        this.index = index;
        long numRows = table.getNumRows();
        long partitionSize = numRows / numPartitions;
        offset = index * partitionSize;            // from this row index (inclusive)
        limit = (index + 1) * partitionSize;       // to this row index (exclusive)
        if (limit > numRows) {
            limit = numRows;
        }
        tableId = table.getId();
    }

    @Override
    public int index() {
        return index;
    }

    @Override
    public int hashCode() {
        return index;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof TablePartition && ((TablePartition) obj).index == index);
    }

    public SparkTablePartitionIterator getIterator() {
        return new SparkTablePartitionIterator(Table.getTableById(tableId), offset, limit);
    }
}
