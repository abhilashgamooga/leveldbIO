import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Iterator;

import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import static org.fusesource.leveldbjni.JniDBFactory.*;

public class LevelDBReader {
    DB db;
    DB writeDb;

    public LevelDBReader() throws IOException {
        File dbPath = Paths.get("/data/master/0/2019-9/").toFile();
        File dbWritePath = Paths.get("/data/master/0/2040-9/").toFile();

        Options options = new Options();
        options.createIfMissing(true);
        options.writeBufferSize(256 << 20);
        options.blockSize(64 * 1024);
        this.db = factory.open(dbPath, options);
        this.writeDb = factory.open(dbWritePath, options);
    }

    public void start() {
        DBIterator iterator = this.db.iterator();
        int i =0;
        System.out.println("Leveldb Read Start - "+System.currentTimeMillis());
        for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
            if(i > 10000000) {
                i=0;
                System.out.println("read 10000000");
            }
            String key = asString(iterator.peekNext().getKey());
            byte[] val = iterator.peekNext().getKey();
            String[] props = key.split(":");
            if(props.length != 4) {
                System.out.println("got invalid data {}"+props.toString());
                continue;
            }
            String newKey = props[0]+":"+props[1]+":"+props[3]+":"+props[2];
            this.writeDb.put(bytes(newKey), val);

            ++i;
        }
        System.out.println("Leveldb Read End - "+System.currentTimeMillis());
    }
    public static void main(String[] args) {
        try {
            System.out.println("data write started");
            LevelDBReader ldb = new LevelDBReader();
            ldb.start();
            System.out.println("data write ended");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(" Exception occurred "+e.getMessage());
        }
    }
}
