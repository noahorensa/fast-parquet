import norensa.parquet.io.Parquet;
import norensa.parquet.io.Table;
import norensa.parquet.spark.SparkTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

public class SparkParquetTest {

    @Test
    void test_rdd() {
        Table t = Parquet.read("resources/test_32mx16x32");
        System.out.println(SparkTable.from(t).count());
    }

    @Test
    void test_df() {
        Table t = Parquet.read("resources/test_incorta_datatypes.parquet", "test");
        SparkTable rdd = SparkTable.from(t);
        Dataset<Row> df = rdd.toDF();
        df.printSchema();
        SparkTable.sql("SELECT * FROM test").show();
    }

    @Test
    void test_df1() {
        Dataset<Row> emp = SparkTable.from(Parquet.read("resources/employees", "employee")).toDF();
        Dataset<Row> dep = SparkTable.from(Parquet.read("resources/departments", "department")).toDF();
        SparkTable.sql(
                "SELECT *" +
                        " FROM employee AS emp JOIN department AS dep ON emp.department = dep.id"
        ).show();
    }

    @Test
    void test_df2() {
        Dataset<Row> emp = SparkTable.from(Parquet.read("resources/employees", "employee")).toDF();
        Dataset<Row> dep = SparkTable.from(Parquet.read("resources/departments", "department")).toDF();
        SparkTable.sql(
                "SELECT emp.id, emp.fname, emp.birthday, emp.salary" +
                        " FROM employee AS emp JOIN department AS dep ON emp.department = dep.id" +
                        " WHERE emp.salary>1000"
        ).show();
    }

}
