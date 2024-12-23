import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
// Build Redis
public class Main {
  private static int port;
  private static String role = "master";
  public static Map<String, String> data = new ConcurrentHashMap<>();
  public static Map<String, LocalDateTime> expiryTimes =
      new ConcurrentHashMap<>();
  private static String dir = null;
  private static String dbfilename = null;
  private static String master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
  private static int master_repl_offset = 0;
  public static void loadRDBFile() throws FileNotFoundException, IOException {
    if (dir == null || dbfilename == null) {
      System.out.println("No RDB File specified.");
      return;
    }
    String pathname = dir + '/' + dbfilename;
    java.io.File rdbFile = new java.io.File(pathname);
    if (!rdbFile.exists()) {
      System.out.println("RDB File does not exist at the specified filepath: " +
                         pathname);
      return;
    }
    try (java.io.FileInputStream fileInputStream =
             new java.io.FileInputStream(rdbFile)) {
      // create a buffer to read from the file input stream - set capacity of
      // this buffer to the length of rdbFile
      ByteBuffer fileBuffer = ByteBuffer.allocate((int)rdbFile.length());
      // returns the unique FileChannel object associated with this file input
      // stream.
      FileChannel fileChannel = fileInputStream.getChannel();
      // read a squence of bytes from the channel associated with the file's
      // input stream into the fileBuffer
      fileChannel.read(fileBuffer);
      fileBuffer.flip();
      parseRDB(fileBuffer);
    }
  }
  private static void parseRDB(ByteBuffer fileBuffer) {
    byte[] header = new byte[9];
    fileBuffer.get(header);
    String headStr = new String(header);
    if (!headStr.equals("REDIS0011")) {
      throw new IllegalArgumentException("Invalid RDB Header: " + headStr);
    }
    System.out.println("RDB Header Validated.");
    while (fileBuffer.hasRemaining()) {
      byte sectionType = fileBuffer.get();
      if (sectionType == (byte)0xFE) {
        continue;
      } else if (sectionType == (byte)0x00) {
        String key = readString(fileBuffer);
        String value = readString(fileBuffer);
        if (!key.isEmpty()) {
          data.put(key, value);
        }
      } 
      else if (sectionType == (byte)0xFC) {
        long expiryMillis = readTimeMS(fileBuffer);
        readString(fileBuffer);
        String key = readString(fileBuffer);
        String value = readString(fileBuffer);
        LocalDateTime expirationTime = LocalDateTime.ofEpochSecond(expiryMillis / 1000, (int) (expiryMillis % 1000) * 1_000_000, java.time.ZoneOffset.UTC);
        
        System.out.println("Loaded Key: " + key + " Value: " + value + "Expiry: " + expirationTime);

        if (!key.isEmpty()) {
            data.put(key, value);
            expiryTimes.put(key, expirationTime);
        }
      } else if (sectionType == (byte)0xFD) {
        long expiryTime = readTimeSeconds(fileBuffer);
        readString(fileBuffer);
        String key = readString(fileBuffer);
        String value = readString(fileBuffer);
        LocalDateTime expirationTime = LocalDateTime.ofEpochSecond(expiryTime, 0, java.time.ZoneOffset.UTC);
        
        System.out.println("Loaded Key: " + key + " Value: " + value + "Expiry: " + expirationTime);

        if (!key.isEmpty()) {
            data.put(key, value);
            expiryTimes.put(key, expirationTime);
        }
      } else if (sectionType == (byte)0xFF) {
        break;
      } 
    }
  }

  private static long readTimeSeconds(ByteBuffer fileBuffer) {
    int unsigned_int = fileBuffer.getInt();
    long unixTimeStamp = unsigned_int & 0xFFFFFFFFL;
    return unixTimeStamp;
  }
  
  private static long readTimeMS(ByteBuffer fileBuffer) {
    // Java long is already 8 bytes so no need to mask
    long unsigned_long = fileBuffer.getLong();
    return unsigned_long;
  }
  private static String readString(ByteBuffer fileBuffer) {
    int size = readSize(fileBuffer);
    byte[] StringBytes = new byte[size];
    fileBuffer.get(StringBytes);
    return new String(StringBytes);
  }
  private static int readSize(ByteBuffer buffer) {
    byte firstByte = buffer.get();
    int firstTwoBits = (firstByte & 0xC0) >> 6;
    if (firstTwoBits == 0) {
      // 0b00 - 6-bit size
      return firstByte & 0x3F;
    } else if (firstTwoBits == 1) {
      // 0b01 - 14-bit size (6 bits from first byte + 8 bits from second byte)
      int secondByte = buffer.get() & 0xFF;
      return ((firstByte & 0x3F) << 8) | secondByte;
    } else if (firstTwoBits == 2) {
      // 0b10 - 32-bit size (next 4 bytes as a full integer)
      return buffer.getInt();
    } else if (firstTwoBits == 3) {
      // 0b11 - Special encoding
      int encodingType =
          firstByte & 0x3F; // The remaining 6 bits tell us the type
      switch (encodingType) {
        case 0x00: // 0xC0 - 8-bit integer encoding
            return buffer.get();
        case 0x01: // 0xC1 - 16-bit integer encoding (little-endian)
            return buffer.getShort() & 0xFFFF; // convert short to unsigned int
        case 0x02: // 0xC2 - 32-bit integer encoding (little-endian)
            return buffer.getInt();
        default:
        System.out.println("Unknown encoding type: " + encodingType);
        buffer.position(buffer.position() + 1);
        return 0;
      }
    } else {
      System.out.println("Unsupported size encoding");
      return 0;
    }
  }
  public static void main(String[] args) throws ClosedChannelException {
    try {
        if (args != null && args.length > 0) {
            if ((args[0].equalsIgnoreCase("--port") || args[0].equalsIgnoreCase("-p")) && args.length > 1) {
                port = Integer.parseInt(args[1]);
                if(args.length > 2) {
                    if(args[2].equalsIgnoreCase("--replicaof")) {
                        role = "slave";
                    }
                }
            } else {
                // Default port if --port is not provided
                port = 6379;
            }

            if (args.length > 2 && args[0].equalsIgnoreCase("--dir") && args.length > 3) {
                dir = args[1];
                dbfilename = args[3];
                loadRDBFile();
            }
        } else {
            port = 6379;
        }
      // create Selector for monitoring channels
      Selector selector = Selector.open();
      // create a non-blocking server socket channel
      ServerSocketChannel serverChannel = ServerSocketChannel.open();
      serverChannel.configureBlocking(false);
      // bind the server channel to the specified port and register
      // the channel with the selector
      serverChannel.bind(new InetSocketAddress(port));
      serverChannel.register(selector, SelectionKey.OP_ACCEPT);
      ByteBuffer buffer = ByteBuffer.allocate(256);
      System.out.println("Server is running on port " + port);
      // Event loop
      while (true) {
        // Select ready channels using the selector
        selector.select();
        // Get the set of selected keys corresponding to ready channels
        Set<SelectionKey> selectedKeys = selector.selectedKeys();
        // create an iterator object to iterate through the selectedKeys
        Iterator<SelectionKey> keyIterator = selectedKeys.iterator();
        while (keyIterator.hasNext()) {
          SelectionKey currKey = keyIterator.next();
          if (currKey.isAcceptable()) {
            // if the current selection key corresponds to a channel that is
            // registered with the OP_ACCEPT event then we create a new client
            // connection
            ServerSocketChannel server = (ServerSocketChannel)currKey.channel();
            // because the channel is nonBlocking, we do not hold up the main
            // thread waiting for a new connection
            SocketChannel clientChannel = server.accept();
            if (clientChannel != null) {
              // if there is no client awaiting connection then we immediately
              // return null if not clientChannel is a reference to the
              // connection with the client
              clientChannel.configureBlocking(false);
              // Register for read events
              clientChannel.register(selector, SelectionKey.OP_READ);
              System.out.println("New client connected: " +
                                 clientChannel.getRemoteAddress());
            } else {
              continue;
            }
          } else if (currKey.isReadable()) {
            // check if event on the channel is a READ event
            SocketChannel clientChannel = (SocketChannel)currKey.channel();
            // Read data from the client
            buffer.clear();
            // clears the byte buffer to prepare it for a new read operation
            int bytesRead = clientChannel.read(
                buffer); // .read returns the number of bytes read - if no data
                         // is available returns 0, if client closed connection
                         // return -1
            if (bytesRead == -1) {
              System.out.println("Client disconnected: " +
                                 clientChannel.getRemoteAddress());
              currKey
                  .cancel(); // cancel the key and remove it from the Selector
              clientChannel.close();
              continue; // continue to the next key
            }
            buffer.flip();
            // prepares the ByteBuffer for reading the data it has received -
            // set buffer limit to the current position and resets the position
            // to 0 so we can start reading at the beginning of the buffer
            String message = new String(buffer.array(), 0, bytesRead);
            // buffer.array is the byte array backing the buffer 0 and bytesRead
            // are the starting and ending points of the read
            System.out.println("Message Received " + message);
            String[] parts = message.split("\r\n");
            if (parts.length >= 2) {
              String command = parts[2].toUpperCase();
              ByteBuffer responseBuffer = null;
              switch (command) {
              case "PING":
                responseBuffer = ByteBuffer.wrap("+PONG\r\n".getBytes());
                clientChannel.write(responseBuffer);
                break;
              case "ECHO":
                System.out.println("Echo Argument: " + parts[4]);
                // parts[3] = $[length of echo argument]
                responseBuffer = ByteBuffer.wrap(
                    (parts[3] + "\r\n" + parts[4] + "\r\n").getBytes());
                clientChannel.write(responseBuffer);
                break;
              case "SET":
                // Set command without PX
                System.out.println("Set Command key: " + parts[4] +
                                   "\r\nSet Command value: " + parts[6]);
                if (parts.length == 7) {
                  if (!parts[4].isEmpty()) {
                    data.put(parts[4], parts[6]);
                  }
                  responseBuffer = ByteBuffer.wrap("+OK\r\n".getBytes());
                  expiryTimes.remove(
                      parts[4]); // remove any existing expiration for this key
                } else if (parts.length == 11 &&
                           parts[8].equalsIgnoreCase("PX")) {
                  // Set command with PX to set expiration
                  if (!parts[4].isEmpty()) {
                    data.put(parts[4], parts[6]);
                  }
                  long expiryMillis = 1_000_000 * Long.parseLong(parts[10]);
                  LocalDateTime expiryTime =
                      LocalDateTime.now().plusNanos(expiryMillis);
                  expiryTimes.put(parts[4],
                                  expiryTime); // store the expiration time
                  responseBuffer = ByteBuffer.wrap("+OK\r\n".getBytes());
                }
                clientChannel.write(responseBuffer);
                break;
              case "GET":
                LocalDateTime expirationTime = expiryTimes.get(parts[4]);
                if (expirationTime != null) {
                  if (LocalDateTime.now().isAfter(expirationTime)) {
                    // the case where the access occurs after the expiration
                    // time
                    responseBuffer = ByteBuffer.wrap("$-1\r\n".getBytes());
                    clientChannel.write(responseBuffer);
                    break;
                  }
                }
                String key = data.get(parts[4]);
                if (key == null) {
                  responseBuffer = ByteBuffer.wrap("$-1\r\n".getBytes());
                } else {
                  responseBuffer = ByteBuffer.wrap(
                      ("$" + key.length() + "\r\n" + key + "\r\n").getBytes());
                }
                clientChannel.write(responseBuffer);
                break;
              case "CONFIG":
                String commandStr = null;
                String paramValue = null;
                String prefix = "*2\r\n";
                String parameterName = parts[6];
                if (parts[4].equals("GET")) {
                  // use .equals() for string comparison "==" is for reference
                  // comparison
                  System.out.println("Received CONFIG GET Command");
                  if (parameterName.equals("dir")) {
                    commandStr = "$3\r\ndir\r\n";
                    paramValue = dir != null ? dir : "";
                  } else if (parameterName.equals("dbfilename")) {
                    commandStr = "$10\r\ndbfilename\r\n";
                    paramValue = dbfilename != null ? dbfilename : "";
                  }
                  responseBuffer = ByteBuffer.wrap(
                      (prefix + commandStr + "$" + paramValue.length() +
                       "\r\n" + paramValue + "\r\n")
                          .getBytes());
                  clientChannel.write(responseBuffer);
                }
                break;
              case "KEYS":
                System.out.println("Received KEYS command");
                if (parts[4].equals("*")) {
                  System.out.println("Received KEYS * command");
                  StringBuilder response =
                      new StringBuilder("*").append(data.size()).append("\r\n");
                  data.keySet().forEach(v -> {
                    if (!v.isEmpty()) {
                      response.append("$")
                          .append(v.length())
                          .append("\r\n")
                          .append(v)
                          .append("\r\n");
                    }
                  });
                  clientChannel.write(
                      ByteBuffer.wrap(response.toString().getBytes()));
                }
                break;
              case "INFO": 
                System.out.println("Recieved INFO command");
                clientChannel.write(ByteBuffer.wrap(("$" + (5 + role.length()) + "\r\nrole:" + role + "\r\n" + "$" + (14 + master_replid.length()) + "\r\nmaster_replid:" + master_replid + "\r\n" + "$" + (20) + "\r\nmaster_repl_offset:" + master_repl_offset + "\r\n").getBytes()));
                break;
              default:
                break;
              }
            }
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}