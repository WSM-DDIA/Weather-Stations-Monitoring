package baseCentralStation;

import baseCentralStation.Utilities.BitCaskClient;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BitCaskReader {
    public static void main(String[] args) throws IOException {
        BitCaskClient bitCaskClient = new BitCaskClient();
        bitCaskClient.startConnection("localhost", 4240);

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                for (int i = 1; i <= 10; i++) {
                    System.out.println("initiating get " + i);
                    String response = bitCaskClient.get(i);
                    System.out.println(response);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, 5, 5, TimeUnit.SECONDS);
    }
}
