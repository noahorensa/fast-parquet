package norensa.parquet.io;

import org.apache.parquet.format.*;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.transport.TMemoryInputTransport;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;

class ParquetFileReader implements Runnable {

    private static final int MAX_PAGE_HEADER_SIZE = 256;

    private long fileSize;
    private FileChannel f;
    private int footerLen;
    private FileMetaData fileMetaData;
    private Schema schema;
    //private HashMap<String, Integer> columnOrder;

    private File file;
    private int fileIndex;
    private Parquet parquetReader;

    ParquetFileReader(File file, int fileIndex, Parquet parquetReader) {
        this.file = file;
        this.fileIndex = fileIndex;
        this.parquetReader = parquetReader;
    }

    @Override
    public void run() {
        fileSize = file.length();

        parquetReader.getLogger().info("parquet-io-" + parquetReader.getJobNumber() +
                ": Posting read tasks for " + file.getPath() + ", size = " + fileSize);

        try {
            f = FileChannel.open(file.toPath(), StandardOpenOption.READ);

            if (identifyFile() == false) {
                throw new Exception(String.format("file: %s not a valid Parquet file", file.getPath()));
            }

            readFileMetaData();
            loadDataPages(fileIndex);
            f.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private long calculateOffset(long off) {
        if (off >= 0) {
            return off;
        }
        else {
            return fileSize + off;
        }
    }

    private static int makei32(byte[] buf, int off) {
        return ByteBuffer.wrap(buf, off, 4).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get();
    }

    private boolean identifyFile() throws IOException {
        byte[] buf = new byte[4];

        f.position(0);
        f.read(ByteBuffer.wrap(buf));                     // read magic number

        if (new String(buf).equals("PAR1") == false) {
            return false;
        }

        f.position(calculateOffset(-8));                // read footer length
        f.read(ByteBuffer.wrap(buf));
        footerLen = makei32(buf, 0);

        f.read(ByteBuffer.wrap(buf));           // read magic number

        if (new String(buf).equals("PAR1") == false) {
            return false;
        }

        return true;
    }

    private void readFileMetaData() throws IOException, TException {
        byte[] buf = new byte[footerLen];

        f.position(calculateOffset(-8 -footerLen));
        f.read(ByteBuffer.wrap(buf));

        TCompactProtocol tproto = new TCompactProtocol(
                new TMemoryInputTransport(buf)
        );

        fileMetaData = new FileMetaData();
        fileMetaData.read(tproto);

        parquetReader.getFileMetaData().put(fileIndex, fileMetaData);

        schema = new Schema();
        //columnOrder = new HashMap<>();
        int columnIndex = 0;
        for (SchemaElement e : fileMetaData.schema) {
            if (e.type != null) {
                schema.add(new ColumnIdentifier(columnIndex, e));
                //columnOrder.put(e.name, columnIndex);

                columnIndex++;
            }
        }
        parquetReader.getFileSchemas().put(fileIndex, schema);
    }

    private void loadDataPages(int fileIndex) {
        List<DataPage> processedDataPages = Collections.synchronizedList(new LinkedList<>());
        parquetReader.getCompletedPages().put(fileIndex, processedDataPages);
        parquetReader.getFileRowCounts().put(fileIndex, fileMetaData.num_rows);

        long rowIndex = 0;

        for (RowGroup rg : fileMetaData.row_groups) {
            for (ColumnChunk cc : rg.columns) {
                long columnChunkOff = cc.meta_data.data_page_offset;
                int columnChunkLen = (int)cc.meta_data.total_compressed_size;         // FIXME: dangerous cast

                final ByteBuffer columnChunkBuf = ByteBuffer.allocateDirect(columnChunkLen);

                try {
                    f.position(columnChunkOff);
                    f.read(columnChunkBuf);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                final long ri = rowIndex;
                final ColumnIdentifier cid = schema.getColumn(cc.meta_data.path_in_schema.get(0));
                Runnable chunkProcessor = () -> {
                    DictionaryPage dictionaryPage = null;

                    int bufPos = 0;
                    long i = 0;
                    while (i < cc.meta_data.num_values) {
                        byte[] headerBuf = new byte[MAX_PAGE_HEADER_SIZE];
                        columnChunkBuf.position(bufPos);
                        int pageHeaderBufferLen = MAX_PAGE_HEADER_SIZE;
                        if (columnChunkBuf.remaining() < MAX_PAGE_HEADER_SIZE) {
                            pageHeaderBufferLen = columnChunkBuf.remaining();
                        }
                        columnChunkBuf.get(headerBuf, 0, pageHeaderBufferLen);
                        TCompactProtocol tproto = new TCompactProtocol(
                                new TMemoryInputTransport(headerBuf)
                        );

                        PageHeader pageHeader = new PageHeader();
                        try {
                            pageHeader.read(tproto);
                        } catch (TException e) {
                            throw new RuntimeException(e);
                        }

                        bufPos += tproto.getTransport().getBufferPosition();
                        columnChunkBuf.position(bufPos);
                        ByteBuffer pageBuf = (ByteBuffer)(columnChunkBuf.slice().limit(pageHeader.compressed_page_size));

                        switch (pageHeader.type) {
                            case DATA_PAGE:
                            case DATA_PAGE_V2:
                                long numValues = (pageHeader.type == PageType.DATA_PAGE) ?
                                        pageHeader.data_page_header.num_values :
                                        pageHeader.data_page_header_v2.num_values;

                                DataPage p = new DataPage(pageHeader, pageBuf, cid, cc.meta_data.codec, ri + i, dictionaryPage);
                                parquetReader.getReaderThreadPool().execute(p);
                                processedDataPages.add(p);
                                i += numValues;
                                break;

                            case INDEX_PAGE:
                                break;

                            case DICTIONARY_PAGE:
                                dictionaryPage = new DictionaryPage(pageHeader, pageBuf, cid, cc.meta_data.codec);
                                parquetReader.getReaderThreadPool().execute(dictionaryPage);
                                break;
                        }

                        bufPos += pageHeader.compressed_page_size;
                    }
                };
                parquetReader.getReaderThreadPool().execute(chunkProcessor);
            }

            rowIndex += rg.num_rows;
        }
    }
}
