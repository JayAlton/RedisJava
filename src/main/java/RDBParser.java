import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RDBParser {
    // public static List<String> readRDBFile( String file) {
        static HashMap<String , ValueAndExpiry> map = new HashMap<>();
        public static String[] readRDBFile( String file) {
        List<String> keys = new ArrayList<>();
        String key ="";
        String value = "";
        try{
            InputStream fis = new FileInputStream(new File(file));
            byte[] redis = new byte[5];
            byte[] version = new byte[4];
            fis.read(redis);
            fis.read(version);
            System.out.println("Magic String = " +
                               new String(redis, StandardCharsets.UTF_8));
            System.out.println("Version = " +
                               new String(version, StandardCharsets.UTF_8));
            int b;
          header:
            while ((b = fis.read()) != -1) {
              switch (b) {
              case 0xFF:
                System.out.println("EOF");
                break;
              case 0xFE:
                System.out.println("SELECTDB");
                break;
              case 0xFD:
                System.out.println("EXPIRETIME");
                break;
              case 0xFC:
                System.out.println("EXPIRETIMEMS");
                break;
              case 0xFB:
                System.out.println("RESIZEDB");
                b = fis.read();
                fis.readNBytes(lengthEncoding(fis, b) - 1);
                fis.readNBytes(lengthEncoding(fis, b) - 1);
                break header;
              case 0xFA:
                System.out.println("AUX");
                break;
              }
            }
            b = fis.read();
            System.out.println("header done");
            // now key value pairs
            while ((b = fis.read()) != -1) { // value type
              long timestamp = Long.MAX_VALUE;
              byte bytes[];
              if(b == 0xFF) {
                break;
              }

              // check for expiry
              System.out.println("value-type = " + b);
              // expiry in seconds
              if(b == 253) {
                bytes = fis.readNBytes(4);
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                timestamp = buffer.getLong();
                System.out.println("timestamp : "+ timestamp);
                b = fis.read();
              }

              if(b == 252) {
                bytes = fis.readNBytes(8);
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                timestamp = buffer.getLong();
                System.out.println("timestamp : "+ timestamp);
                b = fis.read();
              }
              // String key = "";
              // String value = "";
              
              b = fis.read();
              System.out.println(" b = " + Integer.toBinaryString(b));
              System.out.println("reading keys");
              int strLength = lengthEncoding(fis, b);
              if (strLength == 1) {
                System.out.println("hier");
                // strLength = b & 00000000_00000000_00000000_00111111;
                strLength = b; // FAAAAAAALSCH
              }
              System.out.println("strLength == " + strLength);
              bytes = fis.readNBytes(strLength);
              key = new String(bytes);
              // // read value
              b = fis.read();
              int valueLength = lengthEncoding(fis, b);
              if (valueLength == 1) {
                // valueLength = b & 00111111;
                valueLength = b; // FAAAAAAALSCH
              }
              bytes = fis.readNBytes(valueLength);
              value = new String(bytes);
              System.out.println("key = " + key);
              System.out.println("value = " + value);
              keys.add(key);
              ValueAndExpiry ve = new ValueAndExpiry(value, timestamp);
              map.put(key, ve);
              
            }
           
        }catch(Exception e) {
            System.out.println("Error in reading file content: " + e.getMessage());
        }
        // keys.remove(0);
        System.out.println("keys: " + keys);
        // System.out.println("map : ");
        // printing map
        for(Map.Entry<String , ValueAndExpiry> entry : map.entrySet()) {
          System.out.println(entry.getKey() + " value: "+ entry.getValue().value + " expiry: "+ entry.getValue().expiry);
        }
        System.out.println(map);

        return keys.toArray(new String[0]);
            // out.printf("*1\r\n$%d\r\n%s\r\n", key.length(), key);
            // out.flush();
    }

    private static String readHeader(FileInputStream inputStream, byte[] buffer) throws IOException {
        byte[] header = new byte[9];
        int bytesRead = inputStream.read(header);
        if (bytesRead != 9) {
            throw new IOException("Invalid RDB file header");
        }
        return new String(header);
    }
     private static int lengthEncoding(InputStream is, int b) throws IOException {
        int length = 100;
        int first2bits = b & 11000000;
        if (first2bits == 0) {
            System.out.println("00");
            length = 1;
        } else if (first2bits == 128) {
            System.out.println("01");
            length = 2;
        } else if (first2bits == 256) {
            System.out.println("10");
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
            buffer.put(is.readNBytes(4));
            buffer.rewind();
            length = 1 + buffer.getInt();
        } else if (first2bits == 256 + 128) {
            System.out.println("11");
            length = 1; // special format
        }
        return length;
    }
 

}