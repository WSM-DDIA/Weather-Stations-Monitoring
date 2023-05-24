package bitCask;

import bitCask.command.Command;
import bitCask.command.CommandFactory;
import bitCask.storage.BitCask;
import bitCask.util.Constants;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final int port = 8080;
    private static BitCask bitCask;
    public static void main(String[] args) throws FileNotFoundException {
        PrintWriter out;
        BufferedReader in;
        bitCask = new BitCask(Constants.dbDirectory);

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                bitCask.mergeAndCompaction();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, 5, 5, TimeUnit.SECONDS);
        try (ServerSocket server = new ServerSocket(port)) {
            Socket client = server.accept();
            System.out.println("connected");
            String input;
            out = new PrintWriter(client.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(client.getInputStream()));
            while ((input = in.readLine()) != null) {
                System.out.println("in");
                Command command = CommandFactory.parseCommand(input);
                command.execute(bitCask);
                out.println(bitCask.get(input.split(" ")[1]));
                System.out.println("out");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}