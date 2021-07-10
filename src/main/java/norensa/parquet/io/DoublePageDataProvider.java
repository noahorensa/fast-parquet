package norensa.parquet.io;

import org.apache.parquet.format.Encoding;

public class DoublePageDataProvider extends PageDataProvider {

    DoublePageDataProvider(DataPage dataPage) throws Exception {
        super(dataPage, false);
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
        if (definitionLevels != null) {
            if (definitionLevels[index] == 1) {
                return unsafe.getDouble(bufAddr + indexMap[index] * 8);
            }
            else {
                return "NULL";
            }
        }
        else {
            return unsafe.getDouble(bufAddr + index * 8);
        }
    }
}
