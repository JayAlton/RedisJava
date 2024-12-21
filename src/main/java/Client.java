import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Client implements Runnable {

    private Socket clientSocket;

    public Client(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    public String set(String key, String value, Long expire, Map<String, String> map, Map<String, Long> expireMap) {
        map.put(key, value);
        if (expire != null) {
            expireMap.put(key, System.currentTimeMillis() + expire);
        }
        return "+OK\r\n";

    }

    public String get(String key, Map<String, String> map, Map<String, Long> expireMap) {
        if (expireMap.containsKey(key)) {
            Long expire = expireMap.get(key);
            if (expire < System.currentTimeMillis()) {
                map.remove(key);
                expireMap.remove(key);
                return "$-1\r\n";
            }
        }
        if (map.containsKey(key)) {
            String value = map.get(key);
            return "$" + value.length() + "\r\n" + value + "\r\n";
        } else {
            return "$-1\r\n";
        }
    }

    private static int lengthEncoding(InputStream is, int b) throws IOException {
        int length = 100;
        int first2bits = b & 11000000;
        if (first2bits == 0) {
            System.out.println("00");
            length = 0;
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

    public void run() {
        Map<String, String> map = new HashMap<String, String>();
        Map<String, Long> expireMap = new HashMap<String, Long>();
        try (
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream());
        ) {
            List<String> elements = new ArrayList<String>();
            int elementCount = 0;
            String line;
            while ((line = in.readLine()) != null) {
                elements.add(line);
                if (elements.size() == 1) {
                    elementCount = 1 + 2 * Integer.parseInt(line.substring(1));
                }
                if (elements.size() == elementCount) {
                    String command = elements.get(2);
                    if (command.equals("ping")) {
                        out.print("+PONG\r\n");
                        out.flush();
                    } else if (command.equals("echo")) {
                        String message = elements.get(4);
                        // $3\r\nhey\r\n
                        out.printf("$%d\r\n%s\r\n", message.length(), message);
                        out.flush();
                    } else if (command.equals("set")) {
                        String key = elements.get(4);
                        String value = elements.get(6);
                        Long expire = null;
                        if (elements.size() == 11) {
                            String expireStr = elements.get(10);
                            expire = Long.parseLong(expireStr);
                        }
                        String str = set(key, value, expire, map, expireMap);
                        out.print(str);
                        out.flush();
                    } else if (command.equals("get")) {
                        String key = elements.get(4);
                        String str = get(key, map, expireMap);
                        out.print(str);
                        out.flush();
                    } else if (command.equals("config")) {
                        String key = elements.get(6);
                        String value = Main.config.get(key);
                        if (value == null) {
                            out.print("$-1\r\n");
                            out.flush();
                        } else {
                            out.printf("*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n", value.length(), value);
                            out.flush();
                        }

                    } else if (command.equals("keys")) {
                        // Assuming you're loading keys from the RDB file
                        String dir = Main.config.get("dir");
                        String dbfilename = Main.config.get("dbfilename");
                        String key = "example_key";  // Example key for now
                        
                        try (InputStream fis = new FileInputStream(new File(dir, dbfilename))) {
                            byte[] redis = new byte[5];
                            fis.read(redis);  // Magic string
                            fis.read(new byte[4]);  // Version info
                            int b;
                            while ((b = fis.read()) != -1) {
                                // Simplified reading process
                                if (b == 0xFF) {  // End of file or special marker
                                    break;
                                }
                                // Example: Assume reading a key with length
                                int keyLength = lengthEncoding(fis);  // Get key length
                                byte[] keyBytes = new byte[keyLength];
                                fis.read(keyBytes);
                                key = new String(keyBytes);
                                break;
                            }
                        }
                        out.printf("*1\r\n$%d\r\n%s\r\n", key.length(), key);
                        out.flush();
                    }
                    
                    elements.clear();
                    elementCount = 0;
                }
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}