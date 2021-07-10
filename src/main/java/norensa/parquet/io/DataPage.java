package norensa.parquet.io;

import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.format.*;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DataPage extends Page {
    private long firstRowIndex;
    private int numValues;
    private PageDataProvider dataProvider;
    private DictionaryPage dictionaryPage;
    private byte[] definitionLevels;

    DataPage(PageHeader pageHeader, ByteBuffer buf, ColumnIdentifier columnIdentifier, CompressionCodec compressionCodec,
             long firstRowIndex, DictionaryPage dictionaryPage) {
        super(pageHeader, buf, columnIdentifier, compressionCodec);
        this.firstRowIndex = firstRowIndex;
        this.dictionaryPage = dictionaryPage;

        if (pageHeader.type == PageType.DATA_PAGE) {
            numValues = pageHeader.data_page_header.num_values;
        }
        else if (pageHeader.type == PageType.DATA_PAGE_V2) {
            numValues = pageHeader.data_page_header_v2.num_values;
        }
        else if (pageHeader.type == PageType.DICTIONARY_PAGE) {
            numValues = pageHeader.dictionary_page_header.num_values;
        }
    }

    void setFirstRowIndex(long firstRowIndex) {
        this.firstRowIndex = firstRowIndex;
    }

    long getFirstRowIndex() {
        return firstRowIndex;
    }

    long getNumValues() {
        return numValues;
    }

    public DictionaryPage getDictionaryPage() {
        return dictionaryPage;
    }

    public byte[] getDefinitionLevels() {
        return definitionLevels;
    }

    Object get(int index) {
        return dataProvider.get(index);
    }

    void dropInternalBuffer() {
        buf = null;
    }

    @Override
    public void run() {
        try {
            decompress();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // read repetition and definition levels
        if (pageHeader.type == PageType.DATA_PAGE) {
            if (columnIdentifier.getRepetitionType() == FieldRepetitionType.OPTIONAL) {
                buf.position(4);            // FIXME: find a better way to skip over repetition levels
                buf = buf.slice();

                // definition levels
                if (pageHeader.data_page_header.definition_level_encoding == Encoding.RLE) {
                    ByteBufferInputStream is = new ByteBufferInputStream(buf);
                    RunLengthBitPackingHybridDecoder decoder = new RunLengthBitPackingHybridDecoder(1, is);
                    definitionLevels = new byte[(int)numValues];
                    int i;
                    try {
                        for (i = 0; i < numValues; ++i) {
                            definitionLevels[i] = (byte)decoder.readInt();
                            if (definitionLevels[i] != 1) {
                                columnIdentifier.setNullable();
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    if (columnIdentifier.isNullable() == false) {
                        definitionLevels = null;
                    }

                    buf.position(is.tell());
                    buf = buf.slice();
                }
            }
        }

        try {
            dataProvider = PageDataProvider.createPageDataProvider(this);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
