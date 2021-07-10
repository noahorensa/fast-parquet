package norensa.parquet.io;

import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.PageHeader;

import java.nio.ByteBuffer;

class DictionaryPage extends Page {

    private PageDataProvider dataProvider;

    DictionaryPage(PageHeader pageHeader, ByteBuffer buf, ColumnIdentifier columnIdentifier, CompressionCodec compressionCodec) {
        super(pageHeader, buf, columnIdentifier, compressionCodec);
    }

    @Override
    public void run() {
        try {
            dataProvider = PageDataProvider.createPageDataProvider(new DataPage(pageHeader, buf, columnIdentifier, compressionCodec,
                    0, null));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    Object decode(int id) {
        return dataProvider.get(id);
    }
}
