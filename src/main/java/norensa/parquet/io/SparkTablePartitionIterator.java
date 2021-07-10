package norensa.parquet.io;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import scala.collection.AbstractIterator;

/**
 * An extension of our Java TableIterator and Scala's AbstractIterator to allow Spark to
 * iterate over the table partitions
 */
public class SparkTablePartitionIterator extends AbstractIterator<Row> {

    private long limit;
    private TableIterator internalIterator;

    public SparkTablePartitionIterator(Table table, long offset, long limit) {
        internalIterator = new TableIterator(table);

        this.limit = limit;

        for (int i = 0; i < table.getNumCols(); ++i) {
            while (offset > internalIterator.currDataPages[i].getFirstRowIndex() + internalIterator.currDataPages[i].getNumValues()) {
                internalIterator.currDataPages[i] = internalIterator.pageIterators[i].next();
            }
        }

        internalIterator.pos = offset;
    }

    @Override
    public boolean hasNext() {
        return (internalIterator.pos < limit);
    }

    @Override
    public Row next() {
        return new GenericRow(internalIterator.next());
    }
}
