package norensa.parquet.io;

import org.apache.parquet.format.Encoding;

public class Int64PageDataProvider extends PageDataProvider {

    Int64PageDataProvider(DataPage dataPage) throws Exception {
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
                return dataTypeFactory.createFrom(unsafe.getLong(bufAddr + indexMap[index] * 8));
            }
            else {
                return "NULL";
            }
        }
        else {
            return dataTypeFactory.createFrom(unsafe.getLong(bufAddr + index * 8));
        }
    }
}
