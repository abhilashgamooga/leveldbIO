import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Iterator;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

public class RockDbReader {
    public void testRockDb() throws RocksDBException {
        RocksDB readDb = RocksDB.open( "/data/rocksdb/master/0/2019-6");
        RocksIterator iter = readDb.newIterator();
        int i=0;
        int j=0;
        iter.seekToFirst();
        System.out.println("start -> "+System.currentTimeMillis());
        while (iter.isValid()) {
            String key = new String(iter.key(), StandardCharsets.UTF_8);
            String val = new String(iter.value(), StandardCharsets.UTF_8);
            if(i > 1000000) {
                System.out.println(" 1m -> "+System.currentTimeMillis());
                i=0;
                ++j;
            }
            if(j > 10) {
                break;
            }
            iter.next();
            ++i;
        }
        readDb.close();
    }

    public void testLeveldb() throws IOException {
        File dbPath = Paths.get("/data/leveldb/master/0/2019-6/").toFile();

        org.iq80.leveldb.Options options = new Options();
        options.createIfMissing(true);
        options.writeBufferSize(256 << 20);
        options.blockSize(64 * 1024);
        options.compressionType(CompressionType.SNAPPY);
        DB db = factory.open(dbPath, options);

        DBIterator iterator = db.iterator();
        int i =0;
        int j =0;
        System.out.println("Leveldb Start - "+System.currentTimeMillis());
        for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
            if(i > 1000000) {
                i=0;
                System.out.println("1m ->"+System.currentTimeMillis());
                ++j;
            }
            if(j > 10) {
                break;
            }
            String key = asString(iterator.peekNext().getKey());
            String val = asString(iterator.peekNext().getKey());
            ++i;
        }
    }

    public static void main(String[] args) {
        RockDbReader prb = new RockDbReader();
        try {
            System.out.println("testing rocks db -------------");
            prb.testRockDb();
            System.out.println("testing rocks db end -------------");
            System.out.println("testing level db -------------");
            prb.testLeveldb();
            System.out.println("testing level db end -------------");
        } catch (RocksDBException | IOException e) {
            e.printStackTrace();
        }

    }
}
