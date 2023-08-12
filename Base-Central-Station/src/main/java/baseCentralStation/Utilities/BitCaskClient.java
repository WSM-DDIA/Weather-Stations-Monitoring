package baseCentralStation.Utilities;

import java.io.*;
import java.net.Socket;

public class BitCaskClient {
    private Socket clientSocket;
    private DataOutputStream out;
    private DataInputStream in;

    /**
     * Starts the connection with the BitCask server.
     *
     * @param ip   the ip address of the server
     * @param port the port of the server
     * @throws IOException if the connection fails
     */
    public void startConnection(String ip, int port) throws IOException {
        clientSocket = new Socket(ip, port);
        out = new DataOutputStream(new BufferedOutputStream(clientSocket.getOutputStream()));
        in = new DataInputStream(new BufferedInputStream(clientSocket.getInputStream()));
    }

    /**
     * Sends a put request to the BitCask server.
     *
     * @param key   the key of the value
     * @param value the value to be stored
     * @return the response of the server
     * @throws IOException if the connection fails
     */
    public String put(int key, String value) throws IOException {
        byte[] bytes = RequestParameterBuilder.bitCaskSetParametersAsBytes(key, value);

        out.write(bytes);
        out.flush();

        int response = in.readInt();

        return response == 200 ? "OK" : "ERROR";
    }

    /**
     * Sends a get request to the BitCask server.
     *
     * @param key the key of the value
     * @return the value of the key
     * @throws IOException if the connection fails
     */
    public String get(int key) throws IOException {
        byte[] bytes = RequestParameterBuilder.bitCaskFirstParametersAsBytes(key, (byte) 1);

        out.write(bytes);
        out.flush();

        while (in.available() <= 0) ;

        byte[] response = in.readNBytes(in.available());

        if (response[0] == -1 && response.length == 1)
            return "NOT FOUND";

        return WeatherStatusMessage.parseProtoBufBytesToValue(response);
    }

    /**
     * Sends a delete request to the BitCask server.
     *
     * @param key the key of the value
     * @return the response of the server
     * @throws IOException if the connection fails
     */
    public String delete(int key) throws IOException {
        byte[] bytes = RequestParameterBuilder.bitCaskFirstParametersAsBytes(key, (byte) 3);

        out.write(bytes);
        out.flush();

        int response = in.readInt();

        return response == 200 ? "OK" : "ERROR";
    }

    /**
     * Sends an open request to the BitCask server.
     *
     * @param directory the directory of the database
     * @throws IOException if the connection fails
     */
    public void open(String directory) throws IOException {
        byte[] bytes = RequestParameterBuilder.bitCaskOpenParametersAsBytes(directory);

        out.write(bytes);
        out.flush();

        int response = in.readInt();

        if (response != 200)
            throw new RuntimeException("Error in opening the database");
    }

    /**
     * Closes the connection with the BitCask server.
     *
     * @throws IOException if the connection fails
     */
    public void stopConnection() throws IOException {
        in.close();
        out.close();
        clientSocket.close();
    }
}
