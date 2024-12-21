import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.HashMap;

public class RequestHandler implements Runnable {

    private Socket clientSocket;
    private String[] args;

    public RequestHandler(Socket clientSocket, String[] args) {
        this.clientSocket = clientSocket;
        this.args = args;
    }

    @Override
public void run() {
    Path dir = Path.of("");
    String dbfilename = "test.rdb";

    if (args.length > 3) {
        if (args[0].equals("--dir")) {
            dir = Path.of(args[1]);
        }
        if (args[2].equals("--dbfilename")) {
            dbfilename = args[3];
        }
    }

    File dbfile = new File(dir.resolve(dbfilename).toString());
    try {
        dbfile.createNewFile();
    } catch (IOException e) {
        throw new RuntimeException(e);
    }

    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
         PrintWriter printWriter = new PrintWriter(clientSocket.getOutputStream(), true);
         InputStream inputStream = new FileInputStream(dbfile)) {
        String clientCommand;
        HashMap<String, String> store = new HashMap<>();
        HashMap<String, LocalDateTime> expiry = new HashMap<>();
        int arrayLen = 0;

        while ((clientCommand = bufferedReader.readLine()) != null) {
            System.out.println("clientCommand: " + clientCommand);
            if (clientCommand.startsWith("*")) {
                // Safely parse arrayLen, ensuring no empty value
                try {
                    arrayLen = Integer.parseInt(clientCommand.substring(1).trim());
                } catch (NumberFormatException e) {
                    printWriter.print("-ERR Invalid array length\r\n");
                    printWriter.flush();
                    continue; // Skip this command and wait for the next
                }
            } else if (Character.isLetter(clientCommand.charAt(0))) {
                switch (clientCommand) {
                    case "ping":
                        printWriter.print("+PONG\r\n");
                        printWriter.flush();
                        break;
                    case "echo":
                        bufferedReader.readLine();
                        String message = bufferedReader.readLine();
                        printWriter.print("$" + message.length() + "\r\n" + message + "\r\n");
                        printWriter.flush();
                        break;
                    case "set":
                        bufferedReader.readLine();
                        String key = bufferedReader.readLine();
                        bufferedReader.readLine();
                        String value = bufferedReader.readLine();
                        int duration_ms = 0;
                        if (arrayLen == 5) {
                            bufferedReader.readLine();
                            bufferedReader.readLine(); // px
                            bufferedReader.readLine();
                            duration_ms = Integer.valueOf(bufferedReader.readLine());
                        }
                        store.put(key, value);
                        if (duration_ms > 0) {
                            expiry.put(key, LocalDateTime.now().plusNanos(duration_ms * 1000000));
                        }
                        printWriter.print("$" + "OK".length() + "\r\n" + "OK" + "\r\n");
                        printWriter.flush();
                        break;
                    case "get":
                        bufferedReader.readLine();
                        String get_key = bufferedReader.readLine();
                        if (!expiry.containsKey(get_key) || expiry.get(get_key).isAfter(LocalDateTime.now())) {
                            String get_value = store.get(get_key);
                            printWriter.print("$" + get_value.length() + "\r\n" + get_value + "\r\n");
                            printWriter.flush();
                        } else {
                            store.remove(get_key);
                            expiry.remove(get_key);
                            printWriter.print("$-1\r\n");
                            printWriter.flush();
                        }
                        break;
                    case "keys":
                        bufferedReader.readLine(); // Read the '*' symbol for the KEYS command
                        String db_op = bufferedReader.readLine();
                        switch (db_op) {
                            case "*":
                                // Send the number of keys in the store (array length)
                                printWriter.print("*" + store.size() + "\r\n");
                                
                                // Iterate over the keys in the store and send each key in the RESP2 format
                                for (String dbKey : store.keySet()) {
                                    printWriter.print("$" + dbKey.length() + "\r\n" + dbKey + "\r\n");
                                }
                                printWriter.flush();
                                break;
                            default:
                                // Handle other operations or invalid commands
                                printWriter.print("-ERR Unknown operation\r\n");
                                printWriter.flush();
                                break;
                        }
                        break;
                                     
                }
            }
        }
    } catch (IOException e) {
        throw new RuntimeException(e);
    }
}

    private static int getLen(InputStream inputStream) throws IOException {
        int read;
        read = inputStream.read();
        int len_encoding_bit = (read & 0b11000000) >> 6;
        int len = 0;
        //System.out.println("bit: " + (read & 0x11000000));
        if (len_encoding_bit == 0) {
            len = read & 0b00111111;
        } else if (len_encoding_bit == 1) {
            int extra_len = inputStream.read();
            len = ((read & 0b00111111) << 8) + extra_len;
        } else if (len_encoding_bit == 2) {
            byte[] extra_len = new byte[4];
            inputStream.read(extra_len);
            len = ByteBuffer.wrap(extra_len).getInt();
        }
        return len;
    }
}