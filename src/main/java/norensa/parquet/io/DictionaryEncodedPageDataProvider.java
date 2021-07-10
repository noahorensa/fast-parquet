package norensa.parquet.io;

import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;

import java.nio.IntBuffer;

class DictionaryEncodedPageDataProvider extends PageDataProvider {

    private int bitWidth;
    private IntBuffer buf;
    private DictionaryPage dictionary;

    DictionaryEncodedPageDataProvider(DataPage dataPage) throws Exception {
        super(dataPage, false);
    }

    @Override
    protected boolean decodePage() throws Exception {
        int numValues = (int)dataPage.getNumValues();

        bitWidth = dataPage.getBuf().get(0);
        dataPage.getBuf().position(1);
        ByteBufferInputStream is = new ByteBufferInputStream(dataPage.getBuf().slice());
        RunLengthBitPackingHybridDecoder decoder = new RunLengthBitPackingHybridDecoder(bitWidth, is);
        buf = IntBuffer.allocate(numValues);
        int i;
        for (i = numValues; i != 0; --i) {
            buf.put(decoder.readInt());
        }

        dataPage.dropInternalBuffer();
        dictionary = dataPage.getDictionaryPage();

        return true;
    }

    @Override
    Object get(int index) {
        if (definitionLevels != null) {
            if (definitionLevels[index] == 1) {
                return dictionary.decode(buf.get(indexMap[index]));
            }
            else {
                return "NULL";
            }
        }
        else {
            return dictionary.decode(buf.get(index));
        }
    }
}
