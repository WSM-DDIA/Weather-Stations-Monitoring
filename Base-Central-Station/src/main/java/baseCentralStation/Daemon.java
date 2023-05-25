package baseCentralStation;
import java.lang.Runnable;


public class Daemon implements Runnable{
    public void run() {
        try {
            CentralStation.invoke();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        Thread t = new Thread(new Daemon());
        t.setDaemon(true);
        t.start();
        t.join();
    }
}