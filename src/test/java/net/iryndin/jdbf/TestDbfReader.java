package net.iryndin.jdbf;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;

import org.junit.Test;

import net.iryndin.jdbf.core.DbfMetadata;
import net.iryndin.jdbf.core.DbfRecord;
import net.iryndin.jdbf.reader.DbfReader;

public class TestDbfReader {

    @Test
    public void test1() throws IOException, ParseException {
        Charset stringCharset = Charset.forName("Cp866");

        File dbf = new File(getClass().getClassLoader().getResource("data1/gds_im.dbf").getPath());

        DbfRecord rec;
        try (DbfReader reader = new DbfReader(dbf)) {
            DbfMetadata meta = reader.getMetadata();

            assertEquals(5, meta.getRecordsQty());
            assertEquals(28, meta.getFields().size());

            System.out.println("Read DBF Metadata: " + meta);
            int recCounter = 0;
            while ((rec = reader.read()) != null) {
                rec.setStringCharset(stringCharset);
                System.out.println("Record is DELETED: " + rec.isDeleted());
                System.out.println(rec.getRecordNumber());
                System.out.println(rec.toMap());

                recCounter++;
                assertEquals(recCounter, rec.getRecordNumber());
            }
        }
    }

    @Test
    public void test2() throws IOException, ParseException {
        Charset stringCharset = Charset.forName("Cp866");

        File dbf = new File(getClass().getClassLoader().getResource("data1/tir_im.dbf").getPath());

        DbfRecord rec;
        try (DbfReader reader = new DbfReader(dbf)) {
            DbfMetadata meta = reader.getMetadata();

            assertEquals(1, meta.getRecordsQty());
            assertEquals(117, meta.getFields().size());

            System.out.println("Read DBF Metadata: " + meta);
            int recCounter = 0;
            while ((rec = reader.read()) != null) {
                rec.setStringCharset(stringCharset);
                System.out.println("Record is DELETED: " + rec.isDeleted());
                System.out.println(rec.getRecordNumber());
                System.out.println(rec.toMap());

                recCounter++;
                assertEquals(recCounter, rec.getRecordNumber());
            }
        }
    }
}
