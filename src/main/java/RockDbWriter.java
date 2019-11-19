import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

public class RockDbWriter {
    private DB readDb;
    private DB writeLevelDb;
    private RocksDB writeDb;
    public RockDbWriter() {
        // a static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();
        File dbPath = Paths.get("/data/master/0/2019-6/").toFile();
        File dbWritePath = Paths.get("/data/leveldb/master/0/2019-6/").toFile();
        org.iq80.leveldb.Options options = new org.iq80.leveldb.Options();
        options.createIfMissing(true);
        options.writeBufferSize(256 << 20);
        options.blockSize(64 * 1024);
        options.compressionType(CompressionType.SNAPPY);
        try {
            this.readDb = factory.open(dbPath, options);
            this.writeLevelDb = factory.open(dbWritePath, options);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
       try {
           this.writeDb = RocksDB.open( "/data/rocksdb/master/0/2019-6");
        } catch (RocksDBException e) {
            // do some error handling
            e.printStackTrace();
        }
    }
    public void start() throws RocksDBException, IOException {
        WriteOptions options = new WriteOptions();
        options.disableWAL();

        DBIterator iterator = this.readDb.iterator();
        int i =0;
        System.out.println("Leveldb Read Start - "+System.currentTimeMillis());
        for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
            if (i > 10000000) {
                i = 0;
                System.out.println("read 10000000");
            }
            byte[] key = (iterator.peekNext().getKey());
            byte[] val = (iterator.peekNext().getKey());
            this.writeDb.put(key, val);
            this.writeLevelDb.put(key, val);
            ++i;
        }
        System.out.println("write completed");


    }

    public void test() throws RocksDBException {
        WriteOptions options = new WriteOptions();
        options.disableWAL();

        this.writeDb.put(options, ".gabhilash".getBytes(), "isgoood".getBytes());
    }
    public static void main(String[] args) {
        RockDbWriter app = new RockDbWriter();
        try{
            //app.test();
            app.start();
        } catch (RocksDBException | IOException e) {
            e.printStackTrace();
        }
    }
}
