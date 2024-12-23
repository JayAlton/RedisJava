
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Parser {
    BufferedReader reader;
    Parser(InputStream inputStream) {
        this.reader = new BufferedReader(new InputStreamReader(inputStream));
    }

    public String[] parseNext() throws IOException {
        String line = reader.readLine();

        if(line == null || line.isEmpty()) {
            return new String[0];
        }

        List<String> tokens = new ArrayList<>();

        if(line.startsWith("*")) {
            int numElements = Integer.parseInt(line.substring(1));
            for(int i = 0; i < numElements ; i++) {
                tokens.add(parseBulkString());
            }
        }

        else if(line.startsWith("$")) {
            tokens.add(parseBulkString());
        }

        else if(line.startsWith("+")) {
            tokens.add(line.substring(1));
        }
        else{
            throw new IOException("Invalid RESP input: " + line);
        }

        return tokens.toArray(new String[0]);
    }

    private String parseBulkString() throws IOException{

        String lengthLine = reader.readLine();

        int length = Integer.parseInt(lengthLine.substring(1));
        if(length == -1) {
            return null;
        }

        String value = reader.readLine();
        return value;
    }
    // comment added
    
}
