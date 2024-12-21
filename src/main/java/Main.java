import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
   private static final ConcurrentHashMap<String, String> dataStore = new ConcurrentHashMap<>();
   private static final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
   private static String dir =  null;
   private static String fileName = null;
   private static InputStream inputStream = null;
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("--dir") && i + 1 < args.length) {
          dir = args[i + 1];
      } else if (args[i].equals("--dbfilename") && i + 1 < args.length) {
          fileName = args[i + 1];
      }
    }
    int port = 6379;
    
    try (ServerSocket serverSocket = new ServerSocket(port)){
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      File rdbFile = new File(dir + "/" + fileName);
      if(!rdbFile.exists()) {
        System.out.println("RDB file not found: " + rdbFile.getPath());
        return;
      }

      inputStream = new FileInputStream(rdbFile);
      // Wait for connections from clients.
      while (true) {
        Socket clientSocket = serverSocket.accept();
        new Thread(() -> handleClient(clientSocket)).start();
        
      }
      
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }

  private static void handleClient(Socket clientSocket) {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
         BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))) {

        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println("::" + line);
            if (line.startsWith("*")) {
                // Multi-bulk request (e.g., SET key value)
                int numArgs = Integer.parseInt(line.substring(1));
                String[] args = new String[numArgs];
                for (int i = 0; i < numArgs; i++) {
                    reader.readLine(); // Read length line (e.g., $3)
                    args[i] = reader.readLine(); // Read actual argument
                }

                processCommand(args, writer);
            } else {
                writer.write("-ERR Unknown command\r\n");
                writer.flush();
            }
        }
    } catch (IOException e) {
        System.out.println("Client disconnected: " + e.getMessage());
    }
  }

  private static void processCommand(String[] args, BufferedWriter writer) throws IOException {
      if (args.length < 1) {
          writer.write("-ERR Missing command\r\n");
          writer.flush();
          return;
      }

      String command = args[0].toUpperCase();
      switch (command) {
          case "ECHO" :
            if(args.length != 2) {
              writer.write("-ERR Wrong number of arguments for ECHO\r\n");
            } else {
              writer.write("+" + args[1] + "\r\n");
            }
            break;
          case "PING":
            if(args.length != 1) {
              writer.write("-ERR Wrong number of arguments for PING");
            } else {
              writer.write("+PONG\r\n");
            }
            break;
          case "SET":
              if (args.length != 3 && args.length != 5) {
                  writer.write("-ERR Wrong number of arguments for SET\r\n");
              } else if (args.length == 3) {
                  dataStore.put(args[1], args[2]);
                  writer.write("+OK\r\n");
              } else if (args.length == 5) {
                dataStore.put(args[1], args[2]);
                writer.write("+OK\r\n");
                
                if(args[3].equalsIgnoreCase("px")){
                  long delay = Long.parseLong(args[4]);
                  executorService.schedule(
                    () -> dataStore.remove(args[1]), delay, TimeUnit.MILLISECONDS);
                  
                }
              }
              break;

          case "GET":
              if (args.length != 2) {
                  writer.write("-ERR Wrong number of arguments for GET\r\n");
              } else {
                  String value = dataStore.get(args[1]);
                  if (value != null) {
                      writer.write("$" + value.length() + "\r\n" + value + "\r\n");
                  } else {
                      writer.write("$-1\r\n");
                  }
              }
              break;

          case "CONFIG":
              if(args[1].equalsIgnoreCase("GET")) {
                String ans = "*2\r\n";
                if(args[2].equalsIgnoreCase("dir")) {
                  ans += "$3\r\ndir\r\n";
                  ans += "$" + dir.length() + "\r\n" + dir + "\r\n";
                } else if (args[2].equalsIgnoreCase("dbfilename")) {
                  ans += "$10\r\ndbfilename\r\n";
                  ans += "$" + fileName.length() + "\r\n" + fileName + "\r\n";
                }
                writer.write(ans);
              }
              break;
          case "KEYS":
            if (args.length != 2 || !"*".equals(args[1])) {
              writer.write("-ERR Only '*' pattern is supported\r\n");
            }
            String key = "foo";
            byte[] redis = new byte[5];
            byte[] version = new byte[4];
            inputStream.read(redis);
            inputStream.read(version);
            System.out.println("Magic String = " + new String(redis, StandardCharsets.UTF_8));
            System.out.println("Version = " + new String(version, StandardCharsets.UTF_8));
            int b;
            header:
            while((b = inputStream.read()) != -1) {
              switch(b) {
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
                  b = inputStream.read();
                  inputStream.readNBytes(getLength(inputStream, b));
                  inputStream.readNBytes(getLength(inputStream, b));
                  break header;
                case 0xFA:
                  System.out.println("AUX");
                  break;
              }
            }
            System.out.println("header done");
            // now key value pairs
            while((b = inputStream.read()) != -1) {
              System.out.println("value-type = " + b);
              b = inputStream.read();
              System.out.println("value-type = " + b);
              System.out.println(" b = " + Integer.toBinaryString(b));
              System.out.println("reading keys");
              int strLength = getLength(inputStream, b);
              b = inputStream.read();
              System.out.println("strLength == " + strLength);
              if (strLength == 0) {
                strLength = b;
              }
              System.out.println("strLength == " + strLength);
              byte[] bytes = inputStream.readNBytes(strLength);
              key = new String(bytes);
              break;
            }
            String outputString = new String("*1\r\n$" + key.length() + "\r\n" + key + "\r\n");
            writer.write(outputString);
            break;
          default:
              writer.write("-ERR Unknown command\r\n");
      }
      writer.flush();
    }
    
    private static int getLength(InputStream is, int b) throws IOException {
      int length = 100;
      int first2bytes = b & 11000000;
      if (first2bytes == 0) {
        System.out.println("00");
        length = 0;
      } else if (first2bytes == 128) {
        System.out.println("01");
        length = 2;
      } else if (first2bytes == 256) {
        System.out.println("10");
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.put(is.readNBytes(4));
        buffer.rewind();
        length = 1 + buffer.getInt();
      } else if (first2bytes == 256 + 128) {
        System.out.println("11");
        length = 1; //special format
      }

      return length;
    }
}

