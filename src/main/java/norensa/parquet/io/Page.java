package norensa.parquet.io;

import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.xerial.snappy.Snappy;

import java.nio.ByteBuffer;

abstract class Page implements Runnable {
    protected PageHeader pageHeader;
    protected ByteBuffer buf;
    protected ColumnIdentifier columnIdentifier;
    protected CompressionCodec compressionCodec;

    Page(PageHeader pageHeader, ByteBuffer buf, ColumnIdentifier columnIdentifier, CompressionCodec compressionCodec) {
        this.pageHeader = pageHeader;
        this.buf = buf;
        this.columnIdentifier = columnIdentifier;
        this.compressionCodec = compressionCodec;
    }

    PageHeader getPageHeader() {
        return pageHeader;
    }

    ByteBuffer getBuf() {
        return buf;
    }

    ColumnIdentifier getColumnIdentifier() {
        return columnIdentifier;
    }

    int getColumnIndex() {
        return columnIdentifier.getIndex();
    }

    Encoding getEncoding() {
        switch (pageHeader.type) {
            case DATA_PAGE:
                return pageHeader.data_page_header.encoding;

            case INDEX_PAGE:
                return null;

            case DICTIONARY_PAGE:
                //return pageHeader.dictionary_page_header.encoding;
                // workaround for older parquet implementations dictionary pages should be PLAIN
                return Encoding.PLAIN;

            case DATA_PAGE_V2:
                return pageHeader.data_page_header_v2.encoding;

            default:
                return null;
        }
    }

    void decompress() throws Exception {
        switch (compressionCodec) {
            case UNCOMPRESSED:
                break;

            case SNAPPY:
                ByteBuffer o = ByteBuffer.allocateDirect(pageHeader.uncompressed_page_size);
                o.limit(Snappy.uncompress(buf, o));
                buf = o;
                break;

            default:
                throw new Exception(String.format("Unsupported compression %s(%d)", compressionCodec, compressionCodec.getValue()));
        }
    }
}
