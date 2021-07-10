import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Test;

import java.io.FileReader;
import java.io.IOException;

class CSVReadTest {

    @Test
    void test_2mx16() {
        long t0 = System.nanoTime();

        try {
            CSVFormat.DEFAULT.parse(new FileReader("resources/test_2mx16.csv")).getRecords();
        } catch (IOException e) {
            e.printStackTrace();
        }

        long t1 = System.nanoTime();
        System.out.println("finished load in " + (t1 - t0) / 1e9 + "s");
    }

    @Test
    void test_8mx16() {
        long t0 = System.nanoTime();

        try {
            CSVFormat.DEFAULT.parse(new FileReader("resources/test_8mx16.csv")).getRecords();
        } catch (IOException e) {
            e.printStackTrace();
        }

        long t1 = System.nanoTime();
        System.out.println("finished load in " + (t1 - t0) / 1e9 + "s");
    }
}