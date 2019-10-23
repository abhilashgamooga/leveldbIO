import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Iterator;

import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

public class LevelDBReader {
    DB db;

    public LevelDBReader() throws IOException {
        File dbPath = Paths.get("/data/master/0/2019-1/").toFile();

        Options options = new Options();
        options.createIfMissing(true);
        options.writeBufferSize(256 << 20);
        options.blockSize(64 * 1024);
        this.db = factory.open(dbPath, options);
    }

    public void start() {
        DBIterator iterator = this.db.iterator();
        int i =0;
        System.out.println("Leveldb Read Start - "+System.currentTimeMillis());
        for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
            if(i > 1000000)
                break;
            ++i;
        }
        System.out.println("Leveldb Read End - "+System.currentTimeMillis());
    }
    public static void main(String[] args) {
        try {
            LevelDBReader ldb = new LevelDBReader();
            ldb.start();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(" Exception occurred "+e.getMessage());
        }
    }
}
