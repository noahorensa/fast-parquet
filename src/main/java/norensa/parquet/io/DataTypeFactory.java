package norensa.parquet.io;

import java.math.BigInteger;

class DataTypeFactory {
    Object createFrom(boolean element) {
        return element;
    }

    Object createFrom(int element) {
        return element;
    }

    Object createFrom(long element) {
        return element;
    }

    Object createFrom(BigInteger element) {
        return element;
    }

    Object createFrom(float element) {
        return element;
    }

    Object createFrom(double element) {
        return element;
    }

    Object createFrom(byte[] element) {
        return element;
    }
}
