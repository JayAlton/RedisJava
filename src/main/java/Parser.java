import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Parser {

    private final List<String> decodedResponse = new ArrayList<>();
    private final String source;

    public Parser() {
        this.source = "";
    }

    public Parser(String source) {
        this.source = source;
    }

    public List<String> getDecodedResponse() {
        return this.decodedResponse;
    }

    public void parse() {
        String[] intermediate = this.source.split("\r\n");
        String[] sourceList = Arrays.copyOf(intermediate, intermediate.length - 1) ;

        parseSourceList(sourceList);
    }

    private void parseSourceList(String[] sourceList) {
        for (String element : sourceList) {
            if (element.charAt(0) == '*') continue;

            if (element.charAt(0) == '$') continue;

            if (element.charAt(0) == ':') continue;

            this.decodedResponse.add(element);
        }
    }

    public static List<String> decodeArray(String[] encodedArray) {
        List<String> result = new ArrayList<>();
        for (String element : encodedArray) {
            if (element.charAt(0) == '*') continue;

            if (element.charAt(0) == '$') continue;

            if (element.charAt(0) == ':') continue;

            result.add(element);
        }

        return result;
    }

    public static String encodeBulkString(String decoded) {
        return "$" + decoded.length() + "\r\n" + decoded + "\r\n";
    }

    public static String encodeArray(Map<String, String> map) {
        Set<String> keys = map.keySet();
        String result = "*" + map.size() * 2 + "\r\n";
        for (String key : keys) {
            int keyLength = key.length();
            String value = map.get(key);
            int valueLength = value.length();
            result += "$" + keyLength + "\r\n" + key + "\r\n$" + valueLength + "\r\n" + value + "\r\n";
        }

        return result;
    }

    public static String encodeArray(Set<String> set) {
        String result = "*" + set.size() + "\r\n";
        for (String key : set) {
            int keyLength = key.length();
            result += "$" + keyLength + "\r\n" + key + "\r\n";
        }

        return result;
    }

    public static String encodeArray(List<String> list) {
        String result = "*" + list.size() + "\r\n";
        for (String key : list) {
            int keyLength = key.length();
            result += "$" + keyLength + "\r\n" + key + "\r\n";
        }

        return result;
    }

    public static HashMap<String, KeyValue> parseRdbFile(byte[] bytes) {
        ArrayList<String> hexFile = bytesToHex(bytes);

        System.out.println(hexFile);

        String header = extractHeader(hexFile);

        String metadata = extractMetadata(hexFile);

        HashMap<String, KeyValue> database = extractDatabase(hexFile);

        hexFile.remove(0);
        ArrayList<String> endOfFile = hexFile;

//        System.out.println(header);
//        System.out.println(metadata);
//        System.out.println(database);
//        System.out.println(endOfFile);

        return database;
    }

    private static ArrayList<String> bytesToHex(byte[] bytes) {
        ArrayList<String> hexResult = new ArrayList<>();

        for (byte b : bytes) {
            hexResult.add(String.format("%02X", b));
        }

        return hexResult;
    }

    private static String extractHeader(ArrayList<String> hexFile) {
        StringBuilder header = new StringBuilder();

        for (String hex : hexFile) {
            if (hex.equals("FA")) break;

            int decimalValue = Integer.parseInt(hex, 16);
            header.append(Character.toChars(decimalValue));
        }

        if (!header.isEmpty()) {
            hexFile.subList(0, header.length()).clear();
        }

        return header.toString();
    }

    private static String extractMetadata(ArrayList<String> hexFile) {
        StringBuilder metadata = new StringBuilder();

        for (int i = 0; i < hexFile.size(); i++) {
            String hex = hexFile.get(i);

            if (hex.equals("FE")) break;
            if (hex.equals("FA")) continue;
            if (hex.equals("05")) {
                metadata.append(" ");
                continue;
            }
            if (hex.equals("C0")) {
                int number = Integer.parseInt(hexFile.get(i + 1), 16);
                metadata.append(" ");
                metadata.append(number);
                hexFile.remove(0);
                continue;
            }

            int decimalValue = Integer.parseInt(hex, 16);
            metadata.append(Character.toChars(decimalValue));

            //System.out.println("hex = " + hex + "; char = " + Arrays.toString(Character.toChars(decimalValue)));
        }

        if (!metadata.isEmpty()) {
            hexFile.subList(0, metadata.length()).clear();
        }

        return metadata.toString();
    }

    private static HashMap<String, KeyValue> extractDatabase(ArrayList<String> hexFile) {
        // need to implement errors
        HashMap<String, KeyValue> result = new HashMap<>();
       // System.out.println(hexFile);

        hexFile.remove(0); // delete hex "FE"
        int databaseIndex = Integer.parseInt(hexFile.remove(0), 16); // get and delete database index

        hexFile.remove(0); // delete hex "FB"
        int keysTableSize = Integer.parseInt(hexFile.remove(0), 16); // get and delete size of the hash table that stores the keys and values (size encoded)
        int expiresTable = Integer.parseInt(hexFile.remove(0), 16); // get and delete size of the hash table that stores the expires of keys (size encoded)

        System.out.println("keys table size = " + keysTableSize);
        System.out.println("expiresTable size = " + expiresTable);

        while (!hexFile.get(0).equals("FF")) {
            KeyValue value = new KeyValue("", 0);

            //System.out.println(hexFile);
            if (hexFile.get(0).equals("FC")) {
                hexFile.remove(0);
                List<String> timestampHex = hexFile.subList(0, 8);

                BigInteger decodedTimestamp = decodeTimestamp(timestampHex);

                value.expiryTimestamp = decodedTimestamp.longValue();

                hexFile.subList(0, 8).clear();
            }

            String flag = hexFile.remove(0); // get and delete flag
            System.out.println("flag = " + flag);
            //System.out.println(hexFile);

            int keySize = Integer.parseInt(hexFile.remove(0), 16);
            List<String> keyHex = hexFile.subList(0, keySize);
            String key = parseHexString(keyHex, keySize);
            //System.out.println(keyHex);

            hexFile.subList(0, keySize).clear();

            int valueSize = Integer.parseInt(hexFile.remove(0), 16);
            List<String> valueHex = hexFile.subList(0, valueSize);
            value.value = parseHexString(valueHex, valueSize);
            //System.out.println(valueHex);

            hexFile.subList(0, valueSize).clear();

            result.put(key, value);
        }

        System.out.println(result);
        return result;
    }

    private static String parseHexString(List<String> hexFile, int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int decimalValue = Integer.parseInt(hexFile.get(i), 16);
            sb.append(Character.toChars(decimalValue));
        }
        return sb.toString();
    }

    private static BigInteger decodeTimestamp(List<String> hexList) {
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
            bytes[7 - i] = (byte) Integer.parseInt(hexList.get(i), 16);
        }

        return new BigInteger(1, bytes);
    }
}