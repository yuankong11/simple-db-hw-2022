package simpledb.common;

enum LogLevel {
    DEBUG, INFO, WARN, ERROR;

    boolean enable() {
        return compareTo(Log.LEVEL) >= 0;
    }
}

public class Log {
    public static final LogLevel LEVEL = LogLevel.INFO;

    private static void log(LogLevel l, String msg, Object... objs) {
        if (l.enable()) {
            System.out.printf((msg) + "%n", objs);
        }
    }

    public static void debug(String msg, Object... objs) {
        log(LogLevel.DEBUG, msg, objs);
    }

    public static void info(String msg, Object... objs) {
        log(LogLevel.INFO, msg, objs);
    }

    public static void warn(String msg, Object... objs) {
        log(LogLevel.WARN, msg, objs);
    }

    public static void error(String msg, Object... objs) {
        log(LogLevel.ERROR, msg, objs);
    }
}
