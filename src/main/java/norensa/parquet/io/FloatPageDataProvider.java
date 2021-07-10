package norensa.parquet.io;

import org.apache.parquet.format.Encoding;

public class FloatPageDataProvider extends PageDataProvider {

    FloatPageDataProvider(DataPage dataPage) throws Exception {
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
                return unsafe.getFloat(bufAddr + indexMap[index] * 4);
            }
            else {
                return "NULL";
            }
        }
        else {
            return unsafe.getFloat(bufAddr + index * 4);
        }
    }
}
