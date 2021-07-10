package norensa.parquet.io;

import java.nio.ByteBuffer;

public class BooleanPageDataProvider extends PageDataProvider {

    private ByteBuffer buf;

    BooleanPageDataProvider(DataPage dataPage) throws Exception {
        super(dataPage, false);
    }

    @Override
    protected boolean decodePage() {
        buf = dataPage.getBuf();
        return false;
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

        int byteIndex = realIndex >> 3;
        return ((buf.get(byteIndex) & (1 << (realIndex - byteIndex << 3))) != 0);
    }
}
