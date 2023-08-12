package baseCentralStation;

import baseCentralStation.Utilities.BitCaskClient;

import java.io.IOException;
import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BitCaskDeleter {
    public static void main(String[] args) throws IOException {
        BitCaskClient bitCaskClient = new BitCaskClient();
        bitCaskClient.startConnection("localhost", 4240);
        bitCaskClient.open("/home/bazina/IdeaProjects/Weather-Stations-Monitoring/Base-Central-Station/src/main/resources/");

        ScheduledExecutorService scheduledDeleter = Executors.newScheduledThreadPool(2);
        scheduledDeleter.scheduleAtFixedRate(() -> {
            Random random = new Random();
            OptionalInt key = random.ints(1, 10).findFirst();
            if (key.isPresent()) {
                try {
                    System.out.println("initiating delete " + key.getAsInt());
                    String response = bitCaskClient.delete(key.getAsInt());
                    System.out.println(response);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            System.out.println();
        }, 2, 5, TimeUnit.SECONDS);
    }
}
