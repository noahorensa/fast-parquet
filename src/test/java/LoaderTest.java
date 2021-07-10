import norensa.parquet.io.Parquet;
import norensa.parquet.io.Table;
import org.junit.jupiter.api.Test;

class LoaderTest {

    @Test
    void test_2mx16x1() {
        long t0 = System.nanoTime();

        try {
            Parquet.read("resources/test_2mx16x1").exportToCsv("resources/test_2mx16x1_csv");
        } catch (Exception e) {
            e.printStackTrace();
        }

        long t1 = System.nanoTime();
        System.out.println("finished load in " + (t1 - t0) / 1e9 + "s");
    }

    @Test
    void test_2mx16x1_snappy() {
        long t0 = System.nanoTime();

        try {
            Parquet.read("resources/test_2mx16x1_snappy").exportToCsv("resources/test_2mx16x1_snappy_csv");
        } catch (Exception e) {
            e.printStackTrace();
        }

        long t1 = System.nanoTime();
        System.out.println("finished load in " + (t1 - t0) / 1e9 + "s");
    }

    @Test
    void test_8mx16x8() {
        long t0 = System.nanoTime();

        try {
            Parquet.read("resources/test_8mx16x8").exportToCsv("resources/test_8mx16x8_csv");
        } catch (Exception e) {
            e.printStackTrace();
        }

        long t1 = System.nanoTime();
        System.out.println("finished load in " + (t1 - t0) / 1e9 + "s");
    }

    @Test
    void test_8mx16x8_snappy() {
        long t0 = System.nanoTime();

        try {
            Parquet.read("resources/test_8mx16x8_snappy").exportToCsv("resources/test_8mx16x8_snappy_csv");
        } catch (Exception e) {
            e.printStackTrace();
        }

        long t1 = System.nanoTime();
        System.out.println("finished load in " + (t1 - t0) / 1e9 + "s");
    }

    @Test
    void test_16mx16x16() {
        long t0 = System.nanoTime();

        try {
            Parquet.read("resources/test_16mx16x16").exportToCsv("resources/test_16mx16x16_csv");
        } catch (Exception e) {
            e.printStackTrace();
        }

        long t1 = System.nanoTime();
        System.out.println("finished load in " + (t1 - t0) / 1e9 + "s");
    }

    @Test
    void test_16mx16x16_snappy() {
        long t0 = System.nanoTime();

        try {
            Parquet.read("resources/test_16mx16x16_snappy").exportToCsv("resources/test_16mx16x16_snappy_csv");
        } catch (Exception e) {
            e.printStackTrace();
        }

        long t1 = System.nanoTime();
        System.out.println("finished load in " + (t1 - t0) / 1e9 + "s");
    }

    @Test
    void test_32mx16x32() {
        long t0 = System.nanoTime();

        try {
            Parquet.read("resources/test_32mx16x32").exportToCsv("resources/test_32mx16x32_csv");
        } catch (Exception e) {
            e.printStackTrace();
        }

        long t1 = System.nanoTime();
        System.out.println("finished load in " + (t1 - t0) / 1e9 + "s");
    }

    @Test
    void test_32mx16x32_snappy() {
        long t0 = System.nanoTime();
        try {
            Parquet.read("resources/test_32mx16x32_snappy").exportToCsv("resources/test_32mx16x32_snappy_csv");
        } catch (Exception e) {
            e.printStackTrace();
        }

        long t1 = System.nanoTime();
        System.out.println("finished load in " + (t1 - t0) / 1e9 + "s");
    }

    @Test
    void test_64mx16x64() {
        long t0 = System.nanoTime();

        try {
            Parquet.read("resources/test_64mx16x64").exportToCsv("resources/test_64mx16x64_csv");
        } catch (Exception e) {
            e.printStackTrace();
        }

        long t1 = System.nanoTime();
        System.out.println("finished load in " + (t1 - t0) / 1e9 + "s");
    }

    @Test
    void test_64mx16x64_snappy() {
        long t0 = System.nanoTime();

        try {
            Parquet.read("resources/test_64mx16x64_snappy").exportToCsv("resources/test_64mx16x64_snappy_csv");
        } catch (Exception e) {
            e.printStackTrace();
        }

        long t1 = System.nanoTime();
        System.out.println("finished load in " + (t1 - t0) / 1e9 + "s");
    }

    @Test
    void test_datatypes() {
        long t0 = System.nanoTime();

        try {
            Table t = Parquet.read("resources/test_incorta_datatypes.parquet");
            t.exportToCsv("resources/test_incorta_datatypes.csv");
            t.show();
        } catch (Exception e) {
            e.printStackTrace();
        }

        long t1 = System.nanoTime();
        System.out.println("finished load in " + (t1 - t0) / 1e9 + "s");
    }

    @Test
    void test_accesstime() {
        long t0 = System.nanoTime();

        Table t = Parquet.read("resources/test_8mx16x8");

        long t1 = System.nanoTime();
        System.out.println("finished load in " + (t1 - t0) / 1e9 + "s");

        for (Object[] row : t) {
        }

        long t2 = System.nanoTime();
        System.out.println("finished access in " + (t2 - t1) / 1e9 + "s");
    }
}