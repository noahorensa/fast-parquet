package norensa.parquet.spark;

import norensa.parquet.io.ColumnIdentifier;
import norensa.parquet.io.Schema;
import norensa.parquet.io.Table;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

public class SparkTable extends RDD<Row> {

    private static final ClassTag<Row> TABLE_ROW_TAG = ClassManifestFactory$.MODULE$.fromClass(Row.class);
    private static SparkContext sc = null;
    private static SparkSession spark = null;

    public static void registerSparkContext(SparkContext sc) {
        SparkTable.sc = sc;
    }

    public static void registerSparkSession(SparkSession spark) {
        SparkTable.spark = spark;
    }

    private static void createSparkContext() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("parquet-io");

        sc = new SparkContext(conf);
    }

    private static void createSparkSession() {
        spark = SparkSession.builder().getOrCreate();
    }

    public static SparkTable from(Table table) {
        if (sc == null) {
            createSparkContext();
        }

        return new SparkTable(table);
    }

    public static Dataset<Row> sql(String query) {
        if (spark == null) {
            createSparkSession();
        }

        return spark.sql(query);
    }

    private long tableId;
    private int numPartitions;

    private SparkTable(Table table) {
        super(sc, new ArrayBuffer<>(), TABLE_ROW_TAG);
        this.tableId = table.getId();
        this.numPartitions = sc.defaultParallelism();
    }

    public StructType getSparkSchema() {
        Schema schema = Table.getTableById(tableId).getSchema();
        StructField[] fields = new StructField[schema.getNumCols()];

        int i;
        for (i = schema.getNumCols() - 1; i >= 0; --i) {
            ColumnIdentifier col = schema.getColumn(i);

            if (col.getConvertedType() != null) {
                switch (col.getConvertedType()) {
                    case UTF8:
                        fields[i] = DataTypes.createStructField(col.getName(), DataTypes.StringType, col.isNullable());
                        break;

                    case MAP:
                        break;
                    case MAP_KEY_VALUE:
                        break;
                    case LIST:
                        break;

                    case ENUM:
                        break;

                    case DECIMAL:
                        fields[i] = DataTypes.createStructField(col.getName(), DataTypes.createDecimalType(col.getPrecision(), col.getScale()), col.isNullable());
                        break;

                    case DATE:
                        fields[i] = DataTypes.createStructField(col.getName(), DataTypes.DateType, col.isNullable());
                        break;

                    case TIME_MILLIS:
                        break;
                    case TIME_MICROS:
                        break;

                    case TIMESTAMP_MILLIS:
                    case TIMESTAMP_MICROS:
                        fields[i] = DataTypes.createStructField(col.getName(), DataTypes.TimestampType, col.isNullable());
                        break;

                    case UINT_8:
                    case INT_8:
                        fields[i] = DataTypes.createStructField(col.getName(), DataTypes.ByteType, col.isNullable());
                        break;

                    case UINT_16:
                    case INT_16:
                        fields[i] = DataTypes.createStructField(col.getName(), DataTypes.ShortType, col.isNullable());
                        break;

                    case UINT_32:
                    case INT_32:
                        fields[i] = DataTypes.createStructField(col.getName(), DataTypes.IntegerType, col.isNullable());
                        break;

                    case UINT_64:
                    case INT_64:
                        fields[i] = DataTypes.createStructField(col.getName(), DataTypes.LongType, col.isNullable());
                        break;

                    case JSON:
                        break;
                    case BSON:
                        break;
                    case INTERVAL:
                        break;
                }
            }
            else {
                switch (col.getType()) {
                    case BOOLEAN:
                        fields[i] = DataTypes.createStructField(col.getName(), DataTypes.BooleanType, col.isNullable());
                        break;

                    case INT32:
                        fields[i] = DataTypes.createStructField(col.getName(), DataTypes.IntegerType, col.isNullable());
                        break;

                    case INT64:
                        fields[i] = DataTypes.createStructField(col.getName(), DataTypes.LongType, col.isNullable());
                        break;

                    case INT96:
                        fields[i] = DataTypes.createStructField(col.getName(), DataTypes.createDecimalType(), col.isNullable());
                        break;

                    case FLOAT:
                        fields[i] = DataTypes.createStructField(col.getName(), DataTypes.FloatType, col.isNullable());
                        break;

                    case DOUBLE:
                        fields[i] = DataTypes.createStructField(col.getName(), DataTypes.DoubleType, col.isNullable());
                        break;

                    case BYTE_ARRAY:
                    case FIXED_LEN_BYTE_ARRAY:
                        fields[i] = DataTypes.createStructField(col.getName(), DataTypes.BinaryType, col.isNullable());
                        break;
                }
            }
        }

        return DataTypes.createStructType(fields);
    }

    public Table getInternalTable() {
        return Table.getTableById(tableId);
    }

    @Override
    public Iterator<Row> compute(Partition split, TaskContext context) {
        return ((TablePartition)split).getIterator();
    }

    @Override
    public Partition[] getPartitions() {
        Partition[] splits = new TablePartition[numPartitions];
        Table table = Table.getTableById(tableId);
        for (int i = numPartitions - 1; i >= 0; --i) {
            splits[i] = new TablePartition(i, numPartitions, table);
        }
        return splits;
    }

    public Dataset<Row> toDF(){
        if (spark == null) {
            createSparkSession();
        }

        Dataset<Row> df = spark.createDataFrame(this, getSparkSchema());
        df.createOrReplaceTempView(Table.getTableById(tableId).getName());
        return df;
    }
}
