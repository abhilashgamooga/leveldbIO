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
    public RockDbWriter() {
        // a static method that loads the RocksDB C++ library.

    }
    public void start() throws RocksDBException, IOException {
        for(int i=1; i < 8; i++) {
            for(int y = 8; y < 10; y++) {
                RocksDB.loadLibrary();
                File dbPath = Paths.get("/data/master/" + i + "/2019-"+y+"/").toFile();
                org.iq80.leveldb.Options options = new org.iq80.leveldb.Options();
                options.createIfMissing(true);
                options.writeBufferSize(256 << 20);
                options.blockSize(64 * 1024);
                options.compressionType(CompressionType.SNAPPY);
                try {
                    DB readDb = factory.open(dbPath, options);
                    RocksDB writeDb = RocksDB.open("/data/rocksdb/master/" + i + "/2019-"+y+"/");

                    DBIterator iterator = readDb.iterator();
                    int c = 0;
                    System.out.println("Reading "+i+" month "+y+" Start - " + System.currentTimeMillis());
                    for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                        if (c > 10000000) {
                            c = 0;
                            System.out.println("read 10000000");
                        }
                        byte[] key = (iterator.peekNext().getKey());
                        byte[] val = (iterator.peekNext().getValue());
                        writeDb.put(key, val);
                        ++c;
                    }
                    System.out.println("write completed "+i+" month "+y+" ----------------");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


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
