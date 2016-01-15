package net.iryndin.jdbf.reader;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.Arrays;

import net.iryndin.jdbf.core.DbfMetadata;
import net.iryndin.jdbf.core.DbfRecord;
import net.iryndin.jdbf.util.DbfMetadataUtils;

public class DbfReader implements Closeable {

    //private InputStream dbfInputStream;
	private RandomAccessFile dbfRandomAccess;
    private MemoReader memoReader;
    private DbfMetadata metadata;
    private byte[] oneRecordBuffer;
    private int recordsCounter = 0;
    private static final int BUFFER_SIZE = 8192;

    public DbfReader(File dbfFile) throws IOException {
        //this(new FileInputStream(dbfFile));
    	this(new RandomAccessFile(dbfFile, "r"));
    }
    
    public DbfReader(File dbfFile, File memoFile) throws IOException {
        //this(new FileInputStream(dbfFile), new FileInputStream(memoFile));
    	this(new RandomAccessFile(dbfFile, "r"), new RandomAccessFile(memoFile, "r"));
    }
    
    public DbfReader(RandomAccessFile dbfRandomAccess) throws IOException {
    	this.dbfRandomAccess = dbfRandomAccess;
    	readMetadata();
    }
    
    public DbfReader(RandomAccessFile dbfRandomAccess, RandomAccessFile memoRandomAccess) throws IOException {
    	this.dbfRandomAccess = dbfRandomAccess;
    	this.memoReader = new MemoReader(memoRandomAccess);
    	readMetadata();
    }
    
    /*public DbfReader(InputStream dbfInputStream) throws IOException {
        this.dbfInputStream = new BufferedInputStream(dbfInputStream, BUFFER_SIZE);
        readMetadata();
    }

    public DbfReader(InputStream dbfInputStream, InputStream memoInputStream) throws IOException {
        this.dbfInputStream = new BufferedInputStream(dbfInputStream, BUFFER_SIZE);
        this.memoReader = new MemoReader(memoInputStream);
        readMetadata();
    }*/

    public DbfMetadata getMetadata() {
        return metadata;
    }

    private void readMetadata() throws IOException {
        //this.dbfInputStream.mark(1024*1024);
        metadata = new DbfMetadata();
        readHeader();
        DbfMetadataUtils.readFields(metadata, dbfRandomAccess);

        oneRecordBuffer = new byte[metadata.getOneRecordLength()];

        findFirstRecord();
    }

    private void readHeader() throws IOException {
        // 1. Allocate buffer
        byte[] bytes = new byte[16];
        // 2. Read 16 bytes
        dbfRandomAccess.read(bytes);
        // 3. Fill header fields
        DbfMetadataUtils.fillHeaderFields(metadata, bytes);
        // 4. Read next 16 bytes (for most DBF types these are reserved bytes)
        dbfRandomAccess.read(bytes);
    }

    @Override
    public void close() throws IOException {
        if (memoReader != null) {
            memoReader.close();
            memoReader = null;
        }
        if (dbfRandomAccess != null) {
            dbfRandomAccess.close();
            dbfRandomAccess = null;
        }
        metadata = null;
        recordsCounter = 0;
    }

    public void findFirstRecord() throws IOException {
        //seek(dbfInputStream, metadata.getFullHeaderLength());
    	dbfRandomAccess.seek(metadata.getFullHeaderLength());
    }

    private void seek(InputStream inputStream, int position) throws IOException {
        inputStream.reset();
        inputStream.skip(position);
    }
    
    public void seekRecord(int recordNo) throws IOException {
    	if(recordNo < 0 || recordNo >= metadata.getRecordsQty()) throw new ArrayIndexOutOfBoundsException(recordNo);
    	//seek(dbfInputStream, metadata.getFullHeaderLength() + metadata.getOneRecordLength() * recordNo);
    	dbfRandomAccess.seek(metadata.getFullHeaderLength() + metadata.getOneRecordLength() * recordNo);
    }

    public DbfRecord read() throws IOException {
        Arrays.fill(oneRecordBuffer, (byte)0x0);
        int readLength = dbfRandomAccess.read(oneRecordBuffer);

        if (readLength < metadata.getOneRecordLength()) {
            return null;
        }

        return createDbfRecord();
    }

    private DbfRecord createDbfRecord() {
        return new DbfRecord(oneRecordBuffer, metadata, memoReader, ++recordsCounter);
    }
}
