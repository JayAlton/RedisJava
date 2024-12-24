import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        EventLoop eventLoop;

        if (args.length > 3 && !args[0].equals("--port")) {
            String dirName = args[1];
            String dbFileName = args[3];
            System.out.println("1 if");

            eventLoop = new EventLoop(dirName, dbFileName);
        } else if (args.length > 3) {
            int port = Integer.parseInt(args[1]);
            String replicaOf = args[3];
            System.out.println("2 if");

            eventLoop = new EventLoop(port, replicaOf);
        } else if (args.length > 1) {
            int port = Integer.parseInt(args[1]);
            String replicaOf = "";
            System.out.println("3 if");

            eventLoop = new EventLoop(port, replicaOf);
        } else {
            String replicaOf = "";
            System.out.println("else");

            eventLoop = new EventLoop(6379, replicaOf);
        }

        eventLoop.start();
    }
}