package norensa.parquet.io;

import org.apache.parquet.format.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Parquet {
    private Table table;
    private ConcurrentHashMap<Integer, List<DataPage>> completedPages;
    private ConcurrentHashMap<Integer, Long> fileRowCounts;
    private ConcurrentHashMap<Integer, FileMetaData> fileMetaData;
    private ConcurrentHashMap<Integer, Schema> fileSchemas;
    private int jobNumber;
    private Logger logger;
    private ThreadPoolExecutor pageProcessingThreadPool;
    private ThreadPoolExecutor readerThreadPool;

    private Parquet(int numThreads) {
        completedPages = new ConcurrentHashMap<>();
        fileRowCounts = new ConcurrentHashMap<>();
        fileMetaData = new ConcurrentHashMap<>();
        fileSchemas = new ConcurrentHashMap<>();
        jobNumber = generateJobNumber();
        logger = Logger.getLogger("parquet-io-" + jobNumber);

        pageProcessingThreadPool = new ThreadPoolExecutor(numThreads, numThreads,
                Long.MAX_VALUE, TimeUnit.NANOSECONDS, new LinkedBlockingQueue<>()
        );

        readerThreadPool= new ThreadPoolExecutor(numThreads, numThreads * 2,
                Long.MAX_VALUE, TimeUnit.NANOSECONDS, new LinkedBlockingQueue<>()
        );
    }

    ConcurrentHashMap<Integer, List<DataPage>> getCompletedPages() {
        return completedPages;
    }

    ConcurrentHashMap<Integer, Long> getFileRowCounts() {
        return fileRowCounts;
    }

    ConcurrentHashMap<Integer, FileMetaData> getFileMetaData() {
        return fileMetaData;
    }

    ConcurrentHashMap<Integer, Schema> getFileSchemas() {
        return fileSchemas;
    }

    ThreadPoolExecutor getReaderThreadPool() {
        return readerThreadPool;
    }

    int getJobNumber() {
        return jobNumber;
    }

    Logger getLogger() {
        return logger;
    }

    private void loadFile(File file, int fileIndex) {
        readerThreadPool.execute(new ParquetFileReader(file, fileIndex, this));
    }

    private void finalizeTable(int fileCount) throws InterruptedException {
        logger.info("parquet-io-" + jobNumber + ": Waiting for read tasks to finish");

        while (readerThreadPool.getTaskCount() != readerThreadPool.getCompletedTaskCount()) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) { }
        }
        readerThreadPool.shutdown();
        readerThreadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        while (pageProcessingThreadPool.getTaskCount() != pageProcessingThreadPool.getCompletedTaskCount()) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) { }
        }
        pageProcessingThreadPool.shutdown();
        pageProcessingThreadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        logger.info("parquet-io-" + jobNumber + ": Indexing table pages");

        table = new Table();
        for (int i = 0; i < fileCount; ++i) {
            table.append(fileMetaData.get(i).num_rows, fileSchemas.get(i), completedPages.get(i));
        }
    }

    private static int jobCount = 0;
    private static synchronized int generateJobNumber() {
        return jobCount++;
    }

    public static Table read(String path) {
        int numProcessors = Runtime.getRuntime().availableProcessors();

        File f = new File(path);
        Parquet r = new Parquet(numProcessors);

        if (f.isFile()) {
            r.loadFile(f, 0);
            try {
                r.finalizeTable(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            File[] files = f.listFiles((File dir, String name) -> name.toLowerCase().endsWith(".parquet"));
            Arrays.sort(files, Comparator.comparing(File::getPath));

            int i = 0;
            for (File ff : files) {
                r.loadFile(ff, i++);
            }

            try {
                r.finalizeTable(files.length);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        return r.table;
    }

    public static Table read(String path, String tableName) {
        Table t = read(path);
        t.setName(tableName);
        return t;
    }
}
