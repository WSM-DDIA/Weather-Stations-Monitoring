package bitCask;

import bitCask.command.Command;
import bitCask.command.CommandFactory;
import bitCask.exception.InvalidCommandException;
import bitCask.storage.BitCask;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final int port = 4240;
    private static BitCask bitCask;

    public static void main(String[] args) throws FileNotFoundException {
        PrintWriter out;
        BufferedReader in;
        bitCask = new BitCask(args[0]);

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                CommandFactory commandFactory = new CommandFactory();
                bitCask.mergeAndCompaction();
                for (int i = 1; i <= 10; i++) {
                    Command command = commandFactory.parseCommand("get " + i);
                    command.execute(bitCask);
                }
            } catch (IOException | InvalidCommandException e) {
                throw new RuntimeException(e);
            }
        }, 5, 5, TimeUnit.SECONDS);

        try (ServerSocket server = new ServerSocket(port)) {
            CommandFactory commandFactory = new CommandFactory();
            Socket client = server.accept();
            System.out.println("connected");
            String input;
            out = new PrintWriter(client.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(client.getInputStream()));
            while ((input = in.readLine()) != null) {
                Command command = commandFactory.parseCommand(input);
                command.execute(bitCask);
                out.println(bitCask.get(input.split(" ")[1]));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}