package org.example;
import org.example.Utilities.CentralBaseStation;

import java.lang.Runnable;

public class Daemon implements Runnable{
    public void run() {
        try {
            CentralBaseStation.invoke();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        Thread t = new Thread(new Daemon());
        t.setDaemon(true);
        t.start();
    }
}