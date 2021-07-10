package norensa.parquet.io;

import org.apache.parquet.format.Encoding;

public class ByteArrayPageDataProvider extends PageDataProvider {

    private int[] elementPosition;

    ByteArrayPageDataProvider(DataPage dataPage) throws Exception {
        super(dataPage, true);
    }

    @Override
    protected boolean decodePage() {
        if (dataPage.getEncoding() != Encoding.PLAIN) {
            return false;
        }

        int numValues = (int) dataPage.getNumValues();
        elementPosition = new int[numValues + 1];

        int pos = 0;
        int i;
        if (definitionLevels != null) {
            for (i = 0; i < numValues; ++i) {
                if (definitionLevels[i] == 1) {
                    elementPosition[i] = pos + 4;
                    pos += unsafe.getInt(bufAddr + pos) + 4;
                }
                else {
                    --i;
                }
            }
        }
        else {
            for (i = 0; i < numValues; ++i) {
                elementPosition[i] = pos + 4;
                pos += unsafe.getInt(bufAddr + pos) + 4;
            }
        }
        elementPosition[i] = pos + 4;

        return true;
    }

    @Override
    Object get(int index) {
        int realIndex = index;
        if (definitionLevels != null) {
            if (definitionLevels[index] != 1) {
                return "NULL";
            }
            realIndex = indexMap[index];
        }

        int elementLen = elementPosition[realIndex + 1] - elementPosition[realIndex] - 4;
        byte[] element = new byte[elementLen];
        long addr = bufAddr + elementPosition[realIndex] - 1;
        int i = 0;
        for (; i < elementLen; ++i) {
            element[i] = unsafe.getByte(++addr);
        }
        return dataTypeFactory.createFrom(element);
    }
}
