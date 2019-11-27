import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.stream.IntStream;

import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.json.JSONArray;
import org.json.JSONObject;

import static org.fusesource.leveldbjni.JniDBFactory.*;

public class LevelDBReader {
    DB db;
    DB writeDb;

    public LevelDBReader() throws IOException {
        File dbPath = Paths.get("/data/master/0/2019-9/").toFile();
        File dbWritePath = Paths.get("/data/new/master/0/2019-9/").toFile();

        Options options = new Options();
        options.createIfMissing(true);
        options.writeBufferSize(256 << 20);
        options.blockSize(64 * 1024);
        options.compressionType(CompressionType.SNAPPY);
        this.db = factory.open(dbPath, options);
        this.writeDb = factory.open(dbWritePath, options);
    }

    public void start() throws IOException {
        DBIterator iterator = this.db.iterator();
        int i =0;
        System.out.println("Leveldb Read Start - "+System.currentTimeMillis());
        for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
            if(i > 10000000) {
                i=0;
                System.out.println("read 10000000");
            }

            byte[] val = iterator.peekNext().getValue();
            JSONObject valObj = new JSONObject(asString(val));

            this.writeDb.put(iterator.peekNext().getKey(), this.converJsonToNewEncode(valObj));
            ++i;
        }
        System.out.println("Leveldb End - "+System.currentTimeMillis());
    }

    public byte[] converJsonToNewEncode(JSONObject obj) {
        String value = "";
        String key = "";
        for (String keyStr : obj.keySet()) {
            if(!key.equals("")) {
                key += "&";
            }
            Object keyvalue = obj.get(keyStr);
            int type = 0;
            if(keyvalue instanceof Integer) {
                type = 1;
            } else if(keyvalue instanceof Long) {
                type = 2;
            } else if(keyvalue instanceof Double) {
                type = 3;
            } else if(keyvalue instanceof JSONArray) {
                type = 4;
            } else if(keyvalue instanceof Boolean) {
                type = 5;
            } else {
                type = 0;
            }
            value += keyvalue;
            String newKey = keyStr+"&"+keyvalue.toString().length()+"&"+type;
            key += newKey;
        }
        String returnVal = value+key+"*"+key.length();
        return returnVal.getBytes();
    }

    public JSONObject convertEncodedBytesToJson(byte[] encoded) throws Exception {
        int n = encoded.length;
        int x = n-1;
        int i = 0;
        int keySize = 0;
        while (x > 0) {
            if (encoded[x] == '*') {
                break;
            }
            keySize += (Math.pow(10,i) * (encoded[x] - 48) ) ;
            --x;
            ++i;
        }

        String completeKey = new String(Arrays.copyOfRange(encoded, x - keySize, x));
        String completeVal = new String(Arrays.copyOfRange(encoded, 0, x-keySize));
        System.out.println("complete key -> "+completeKey);

        JSONObject response = new JSONObject();
        int index = 0;
        String[] keyArray = completeKey.split("&");

        for (int j=0; j < keyArray.length; j++) {
            String key = keyArray[j];
            System.out.println(key);
            int size = Integer.parseInt(keyArray[++j]);
            int type = Integer.parseInt(keyArray[++j]);

            switch (type) {
                case 1:
                    response.put(key, Integer.parseInt(completeVal.substring(index, index+size)));
                    break;
                case 2:
                    //long
                    response.put(key, Long.parseLong(completeVal.substring(index, index+size)));
                    break;
                case 3:
                    //double
                    response.put(key, Double.parseDouble(completeVal.substring(index, index+size)));
                    break;
                case 4:
                    // JSONArray
                    response.put(key, new JSONArray(completeVal.substring(index, index+size)));
                    break;
                case 5:
                    //boolean
                    response.put(key, Boolean.parseBoolean(completeVal.substring(index, index+size)));
                default:
                    //string
                    response.put(key, completeVal.substring(index, index+size));
                    break;
            }
            index += size;
        }

        return response;
    }



    public static void main(String[] args) {
        try {
            System.out.println("data write started");
            LevelDBReader ldb = new LevelDBReader();
//            String test = "521445477wpn193535ad25c&4&0&b44&5&0&c0a&3&0&82a&6&0&609&2&0*39";
//            JSONObject resp = ldb.convertEncodedBytesToJson(test.getBytes());
//            System.out.println(resp);
            ldb.start();
            System.out.println("data write ended");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(" Exception occurred "+e.getMessage());
        }
    }
}
