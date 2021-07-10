package norensa.parquet.io;

import org.apache.parquet.format.Encoding;

public class FixedLenByteArrayPageDataProvider extends PageDataProvider {

    private int elementLen;

    FixedLenByteArrayPageDataProvider(DataPage dataPage) throws Exception {
        super(dataPage, true);
    }

    @Override
    protected boolean decodePage() {
        if (dataPage.getEncoding() != Encoding.PLAIN) {
            return false;
        }

        elementLen = dataPage.columnIdentifier.getElementLength();

        return true;
    }

    @Override
    Object get(int index) {
        int realIndex = index;

        if (definitionLevels != null) {
            if (definitionLevels[index] == 1) {
                realIndex = indexMap[index];
            }
            else {
                return "NULL";
            }
        }

        byte[] element = new byte[elementLen];
        long addr = bufAddr + realIndex * elementLen - 1;
        int i = 0;
        for (; i < elementLen; ++i) {
            element[i] = unsafe.getByte(++addr);
        }
        return dataTypeFactory.createFrom(element);
    }
}
