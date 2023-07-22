package bitCask;

import bitCask.exception.InvalidCommandException;
import bitCask.handler.HandlerFactory;
import bitCask.handler.MessageHandler;
import bitCask.storage.BitCask;

import java.io.*;
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
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(clientSocket.getOutputStream()));
                DataInputStream in = new DataInputStream(new BufferedInputStream(clientSocket.getInputStream()));

                HandlerFactory handlerFactory = new HandlerFactory();
                System.out.println("connected");

                while (!clientSocket.isClosed()) {
                    if (in.available() <= 0)
                        continue;

                    byte[] message = in.readNBytes(in.available());

                    MessageHandler messageHandler = handlerFactory.parseMessage(message);
                    byte[] response = messageHandler.execute(bitCask);

                    if (response == null) {
                        out.write(-1);
                        out.flush();
                    } else {
                        System.out.println("Response is OK");
                        out.write(response);
                        out.flush();
                    }
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
