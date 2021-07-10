package norensa.parquet.io;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class TableIterator implements Iterator<Object[]> {
    protected Table table;
    protected Iterator<DataPage>[] pageIterators;
    protected DataPage[] currDataPages;
    protected long pos;

    TableIterator(Table table) {
        this.table = table;
        List<DataPage>[] pageList = table.getPageList();

        pageIterators = new Iterator[table.getNumCols()];
        for (int i = 0; i < table.getNumCols(); ++i) {
            pageIterators[i] = pageList[i].iterator();
        }

        currDataPages = new DataPage[table.getNumCols()];
        for (int i = 0; i < table.getNumCols(); ++i) {
            currDataPages[i] = pageIterators[i].next();
        }

        pos = 0;
    }

    @Override
    public boolean hasNext() {
        return (pos < table.getNumRows());
    }

    @Override
    public Object[] next() {
        if (hasNext()) {
            Object[] res = new Object[table.getNumCols()];

            for (int i = 0; i < table.getNumCols(); ++i) {
                if (pos - currDataPages[i].getFirstRowIndex() >= currDataPages[i].getNumValues()){
                    currDataPages[i] = pageIterators[i].next();
                }

                DataPage p = currDataPages[i];

                res[i] = p.get((int)(pos - p.getFirstRowIndex()));
            }

            pos++;
            return res;
        }
        else {
            throw new NoSuchElementException("No more elements in table iterator");
        }
    }
}
