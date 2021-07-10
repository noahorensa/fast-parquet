package norensa.parquet.io;

import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageType;
import sun.misc.Unsafe;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.Buffer;
import java.sql.Date;
import java.sql.Timestamp;

abstract class PageDataProvider {
    protected DataPage dataPage;
    protected DataTypeFactory dataTypeFactory;
    protected byte[] definitionLevels;
    protected int[] indexMap;
    protected Unsafe unsafe;
    protected long bufAddr;

    PageDataProvider(DataPage dataPage, boolean withDataTypeFactory) throws Exception {
        this.dataPage = dataPage;

        unsafe = getUnsafe();
        Field f = Buffer.class.getDeclaredField("address");
        f.setAccessible(true);
        bufAddr = f.getLong(dataPage.getBuf());

        if (decodePage() == false) {
            throw new Exception("Error while decoding dataPage");
        }

        decodeDefinitionLevels();

        if (withDataTypeFactory) {
            createDataTypeFactory();
        }
    }

    protected abstract boolean decodePage() throws Exception;

    private void decodeDefinitionLevels() {
        definitionLevels = dataPage.getDefinitionLevels();
        if (definitionLevels != null) {
            int numValues = (int)dataPage.getNumValues();
            indexMap = new int[numValues];
            int in = 0;
            int i;
            int j = 0;

            for (i = 0; i < numValues; ++i) {
                if (definitionLevels[i] == 1) {
                    indexMap[j++] = in++;
                }
                else {
                    indexMap[j++] = 0;
                }
            }
        }
    }

    private void createDataTypeFactory() throws Exception {
        try {
            switch (dataPage.getColumnIdentifier().getConvertedType()) {
                case UTF8:
                    dataTypeFactory = new DataTypeFactory() {
                        @Override
                        Object createFrom(byte[] element) {
                            try {
                                return new String(element, "UTF-8");
                            } catch (UnsupportedEncodingException e) {
                                return null;
                            }
                        }
                    };
                    break;

//                case MAP:
//                    break;
//
//                case MAP_KEY_VALUE:
//                    break;
//
//                case LIST:
//                    break;
//

                case DECIMAL:
                    dataTypeFactory = new DataTypeFactory() {
                        private BigDecimal scale = BigDecimal.TEN.pow(dataPage.getColumnIdentifier().getScale());

                        @Override
                        Object createFrom(byte[] element) {
                            return new BigDecimal(new BigInteger(element)).multiply(scale);
                        }
                    };
                    break;

                case DATE:
                    dataTypeFactory = new DataTypeFactory() {
                        @Override
                        Object createFrom(int element) {
                            return new Date((long)element * 24l * 60l * 60l * 1000l);
                        }
                    };
                    break;

//                case TIME_MILLIS:
//                    break;
//
//                case TIME_MICROS:
//                    break;

                case TIMESTAMP_MILLIS:
                    dataTypeFactory = new DataTypeFactory() {
                        @Override
                        Object createFrom(long element) {
                            return new Timestamp(element);
                        }
                    };
                    break;

                case TIMESTAMP_MICROS:
                    dataTypeFactory = new DataTypeFactory() {
                        @Override
                        Object createFrom(long element) {
                            return new Timestamp(element / 1000);
                        }
                    };
                    break;

                case INT_8:
                case UINT_8:
                    dataTypeFactory = new DataTypeFactory() {
                        @Override
                        Object createFrom(int element) {
                            return (byte) element;
                        }
                    };
                    break;

                case INT_16:
                case UINT_16:
                    dataTypeFactory = new DataTypeFactory() {
                        @Override
                        Object createFrom(int element) {
                            return (short) element;
                        }
                    };
                    break;

//                case JSON:
//                    break;
//
//                case BSON:
//                    break;
//
//                case INTERVAL:
//                    break;

                case ENUM:
                case INT_32:
                case INT_64:
                case UINT_32:
                case UINT_64:
                    dataTypeFactory = new DataTypeFactory();
                    break;

                default:
                    throw new Exception(String.format("Unsupported data type %s", dataPage.columnIdentifier.getConvertedType()));
            }
        } catch (NullPointerException e) {
            dataTypeFactory = new DataTypeFactory();
        }
    }

    abstract Object get(int index);

    static Unsafe getUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe)f.get(null);
        } catch (Exception e) { return null; }
    }

    static PageDataProvider createPageDataProvider(DataPage dataPage) throws Exception {

        if (dataPage.getEncoding() == Encoding.PLAIN_DICTIONARY &&
                dataPage.pageHeader.type != PageType.DICTIONARY_PAGE) {
            return new DictionaryEncodedPageDataProvider(dataPage);
        }
        else {
            switch (dataPage.getColumnIdentifier().getType()) {
                case BOOLEAN:
                    return new BooleanPageDataProvider(dataPage);

                case INT32:
                    return new Int32PageDataProvider(dataPage);

                case INT64:
                    return new Int64PageDataProvider(dataPage);

                case INT96:
                    return new Int96PageDataProvider(dataPage);

                case FLOAT:
                    return new FloatPageDataProvider(dataPage);

                case DOUBLE:
                    return new DoublePageDataProvider(dataPage);

                case BYTE_ARRAY:
                    return new ByteArrayPageDataProvider(dataPage);

                case FIXED_LEN_BYTE_ARRAY:
                    return new FixedLenByteArrayPageDataProvider(dataPage);

                default:
                    return null;
            }
        }
    }
}
