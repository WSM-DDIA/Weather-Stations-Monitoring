package bitCask;

import bitCask.command.Command;
import bitCask.command.CommandFactory;
import bitCask.exception.InvalidCommandException;
import bitCask.storage.BitCask;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class MultiServer {
    private static BitCask bitCask;
    private ServerSocket serverSocket;

    public MultiServer(BitCask bitCask) {
        MultiServer.bitCask = bitCask;
    }

    public void start(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        while (true)
            new EchoClientHandler(serverSocket.accept()).start();
    }

    public void stop() throws IOException {
        serverSocket.close();
    }

    private static class EchoClientHandler extends Thread {
        private final Socket clientSocket;

        public EchoClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        public void run() {
            try {
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(clientSocket.getInputStream()));

                CommandFactory commandFactory = new CommandFactory();
                System.out.println("connected");
                String input;
                while ((input = in.readLine()) != null) {
                    Command command = commandFactory.parseCommand(input);
                    command.execute(bitCask);
                    System.out.println(input);
                    out.println(bitCask.get(input.split(" ")[1]));
                }

                in.close();
                out.close();
                clientSocket.close();
            } catch (IOException | InvalidCommandException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
