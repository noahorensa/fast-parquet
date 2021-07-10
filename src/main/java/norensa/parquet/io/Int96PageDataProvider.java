package norensa.parquet.io;

import org.apache.parquet.format.Encoding;

import java.math.BigInteger;

public class Int96PageDataProvider extends PageDataProvider {

    Int96PageDataProvider(DataPage dataPage) throws Exception {
        super(dataPage, true);
    }

    @Override
    protected boolean decodePage() {
        if (dataPage.getEncoding() != Encoding.PLAIN) {
            return false;
        }

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

        byte[] b = new byte[12];
        long addr = bufAddr + realIndex * 12 - 1;

        int i = 11;
        for (; i >= 0; --i) {
            b[i] = unsafe.getByte(++addr);
        }

        return dataTypeFactory.createFrom(new BigInteger(b));
    }
}
