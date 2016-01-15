package net.iryndin.jdbf;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;

import org.junit.Test;

import net.iryndin.jdbf.core.DbfFieldTypeEnum;
import net.iryndin.jdbf.core.DbfMetadata;
import net.iryndin.jdbf.core.DbfRecord;
import net.iryndin.jdbf.reader.DbfReader;

public class TestMemo {

    @Test
    public void test1() {
        Charset stringCharset = Charset.forName("cp1252");

        File dbf = new File(getClass().getClassLoader().getResource("memo1/texto.dbf").getPath());
        File memo = new File(getClass().getClassLoader().getResource("memo1/texto.fpt").getPath());

        try (DbfReader reader = new DbfReader(dbf, memo)) {
            DbfMetadata meta = reader.getMetadata();
            System.out.println("Read DBF Metadata: " + meta);

            assertEquals(5, meta.getField("TEXVER").getLength());
            assertEquals(DbfFieldTypeEnum.Character, meta.getField("TEXVER").getType());

            assertEquals(4, meta.getField("TEXTEX").getLength());
            assertEquals(DbfFieldTypeEnum.Memo, meta.getField("TEXTEX").getType());

            assertEquals(8, meta.getField("TEXDAT").getLength());
            assertEquals(DbfFieldTypeEnum.Date, meta.getField("TEXDAT").getType());

            assertEquals(1, meta.getField("TEXSTA").getLength());
            assertEquals(DbfFieldTypeEnum.Character, meta.getField("TEXSTA").getType());

            assertEquals(254, meta.getField("TEXCAM").getLength());
            assertEquals(DbfFieldTypeEnum.Character, meta.getField("TEXCAM").getType());

            DbfRecord rec;
            while ((rec = reader.read()) != null) {
                rec.setStringCharset(stringCharset);

                System.out.println("Record is DELETED: " + rec.isDeleted());
                System.out.println("TEXVER: " + rec.getString("TEXVER"));
                System.out.println("TEXTEX: " + rec.getMemoAsString("TEXTEX"));
                System.out.println("TEXDAT: " + rec.getDate("TEXDAT"));
                System.out.println("TEXSTA: " + rec.getString("TEXSTA"));
                System.out.println("TEXCAM: " + rec.getString("TEXCAM"));
                System.out.println("++++++++++++++++++++++++++++++++++");
            }

        } catch (IOException e) {
            //e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
