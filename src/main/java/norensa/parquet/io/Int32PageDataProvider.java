package norensa.parquet.io;

import org.apache.parquet.format.Encoding;

public class Int32PageDataProvider extends PageDataProvider {

    Int32PageDataProvider(DataPage dataPage) throws Exception {
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
        if (definitionLevels != null) {
            if (definitionLevels[index] == 1) {
                return dataTypeFactory.createFrom(unsafe.getInt(bufAddr + indexMap[index] * 4));
            }
            else {
                return "NULL";
            }
        }
        else {
            return dataTypeFactory.createFrom(unsafe.getInt(bufAddr + index * 4));
        }
    }
}
