package baseCentralStation;

import baseCentralStation.Utilities.BitCaskClient;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BitCaskReader {
    /**
     * This method is the main method of the BitCaskReader.
     * It initializes the BitCaskClient and reads messages from the BitCask store.
     *
     * @param args unused
     * @throws IOException if the BitCaskClient fails to initialize
     */
    public static void main(String[] args) throws IOException {
        BitCaskClient bitCaskClient = new BitCaskClient();
        bitCaskClient.startConnection("localhost", 4240);
        bitCaskClient.open("/home/bazina/IdeaProjects/Weather-Stations-Monitoring/Base-Central-Station/src/main/resources/");

        ScheduledExecutorService scheduledReader = Executors.newScheduledThreadPool(2);
        scheduledReader.scheduleAtFixedRate(() -> {
            try {
                for (int i = 1; i <= 10; i++) {
                    System.out.println("initiating get " + i);
                    String response = bitCaskClient.get(i);
                    System.out.println(response);
                }
                System.out.println();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, 2, 5, TimeUnit.SECONDS);
    }
}
