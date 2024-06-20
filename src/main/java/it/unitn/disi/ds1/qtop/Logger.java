package it.unitn.disi.ds1.qtop;

import it.unitn.disi.ds1.qtop.Utils.LogLevel;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;


public class Logger {

    private static Logger instance = null;
    private LogLevel logLevel = LogLevel.INFO;

    private List<String> logs;

    private PrintWriter writer;

    private Logger() {
        logLevel = LogLevel.INFO;
        logs = new ArrayList<>();
        try {
            writer = new PrintWriter("simulation.log", "UTF-8");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static Logger getInstance() {
        if (instance == null) {
            synchronized (Logger.class) {
                if (instance == null) {
                    instance = new Logger();
                }

            }
        }
        return instance;
    }

    public void setLogLevel(LogLevel level) {
        logLevel = level;
    }

    public void log(LogLevel level, String message) {
        if (level.ordinal() >= logLevel.ordinal()) {
            String log = String.format(
                    "[%s] %s",
                    level,
                    message
            );
            writer.println(log);
            writer.flush();
        }
        logs.add(String.format(
                "[%s] %s",
                level,
                message
        ));
    }


}
