package baseCentralStation;

import baseCentralStation.Utilities.BitCaskClient;

import java.io.IOException;
import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BitCaskReader {
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
