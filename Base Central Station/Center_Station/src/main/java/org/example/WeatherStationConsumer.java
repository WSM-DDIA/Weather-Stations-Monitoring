package org.example;

import org.example.Utilities.CentralBaseStation;

public class WeatherStationConsumer {

    public static void main(String[] args) throws Exception {
        CentralBaseStation t = new CentralBaseStation();
        t.invoke();
    }

}
