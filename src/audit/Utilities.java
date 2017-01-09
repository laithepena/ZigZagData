package audit;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public class Utilities {
    public static String getLocalDateID(String x, String timeZoneEventTime) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        formatter.setTimeZone(TimeZone.getTimeZone(timeZoneEventTime));
        long milliseconds = Long.parseLong(x);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(milliseconds);
        String ss = formatter.format(calendar.getTime());
        return ss;
    }

    public static String getLocalDateID_All(String x, String timeZoneEventTime) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HH-mm-ss");
        formatter.setTimeZone(TimeZone.getTimeZone(timeZoneEventTime));
        long milliseconds = Long.parseLong(x);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(milliseconds);
        String ss = formatter.format(calendar.getTime());
        return ss;
    }
}