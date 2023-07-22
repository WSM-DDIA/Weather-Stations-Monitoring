package baseCentralStation.Utilities;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parsing {

    private static final Pattern pattern = Pattern.compile("\\{station_id=(\\d+), s_no=(\\d+), battery_status='(\\w+)', " +
            "status_timestamp=(\\d+), weather=\\{humidity=(\\d+(?:\\.\\d+)?), temperature=(\\d+(?:\\.\\d+)?), wind_speed=(\\d+(?:\\.\\d+)?)}}");

    public Parsing() {}

    public static boolean validate(String input){
        return pattern.matcher(input).matches();
    }

    public static HashMap<String, String> parse(String input){
        Matcher matcher = pattern.matcher(input);
        HashMap<String, String> weatherStatus = new HashMap<>();
        if (matcher.find()) {
            weatherStatus.put("station_id", matcher.group(1));
            weatherStatus.put("s_no", matcher.group(2));
            weatherStatus.put("battery_status", matcher.group(3));
            weatherStatus.put("status_timestamp", matcher.group(4));
            weatherStatus.put("humidity", matcher.group(5));
            weatherStatus.put("temperature", matcher.group(6));
            weatherStatus.put("wind_speed", matcher.group(7));
        }
        return weatherStatus;
    }

}
