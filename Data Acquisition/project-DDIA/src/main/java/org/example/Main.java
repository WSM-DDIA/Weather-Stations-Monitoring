package org.example;

public class Main {
    public static void main(String[] args) {
        // args[0] ==> 1 -> 10
        // args[1] ==> -90 -> 90
        // args[2] ==> -180 -> 180
        System.out.println(args[0] + " " + args[1] + " " + args[2]);
        WeatherStationProducer produce = new WeatherStationProducer(args[0], args[1], args[2]);
//        WeatherStationProducer produce = new WeatherStationProducer("1","50.6","63.7");
        produce.produce();
    }
}