package it.unitn.disi.ds1.qtop;

import it.unitn.disi.ds1.qtop.Utils.LogLevel;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Logger class to log messages to the console and to files.
 */
public class Logger {

    private static Logger instance = null;
    // Map to store PrintWriters for each entity
    private final Map<String, PrintWriter> entityLogs = new HashMap<>();
    private LogLevel logLevel = LogLevel.INFO;
    private PrintWriter info;
    private PrintWriter debug;

    /**
     * Private constructor to prevent instantiation from outside
     */
    private Logger() {
        ensureDirectoryExists("logs"); // Ensure logs directory exists
        try {
            info = new PrintWriter(
                    "logs" + File.separator + "simulation.log",
                    StandardCharsets.UTF_8
            );
            debug = new PrintWriter(
                    "logs" + File.separator + "debug.log",
                    StandardCharsets.UTF_8
            );
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Singleton pattern to get the single instance of Logger.
     *
     * @return the Logger instance
     */
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

    /**
     * Set the log level.
     *
     * @param level level to set
     */
    public void setLogLevel(LogLevel level) {
        logLevel = level;
    }

    /**
     * Ensure the logs directory exists, create if it does not.
     *
     * @param path directory path
     */
    private void ensureDirectoryExists(String path) {
        File directory = new File(path);
        if (!directory.exists()) {
            directory.mkdirs();
        }
    }

    /**
     * Get the PrintWriter for the given entity, create if it does not exist.
     *
     * @param entity entity to get the log for
     *
     * @return PrintWriter for the entity
     */
    private @Nullable PrintWriter getEntityLog(String entity) {
        if (!entityLogs.containsKey(entity)) {
            ensureDirectoryExists("logs");
            try {
                PrintWriter writer = new PrintWriter(
                        "logs" + File.separator + entity + ".log",
                        StandardCharsets.UTF_8
                );
                entityLogs.put(entity, writer);
                return writer;
            } catch (IOException e)
            {
                e.printStackTrace();
                return null;
            }
        }
        return entityLogs.get(entity);
    }

    // Parse the entity (NODE or CLIENT) from the log message

    /**
     * Parse the entity (NODE or CLIENT) from the log message.
     *
     * @param message log message
     *
     * @return the entity "NODE", "CLIENT" or "general"
     */
    private String parseEntity(String message) {
        // Regular expression to match NODE-<number> or CLIENT-<number>
        Pattern pattern = Pattern.compile("(NODE-\\d+)|(CLIENT-\\d+)");
        Matcher matcher = pattern.matcher(message);

        if (matcher.find()) {
            // Return the matched group (NODE or CLIENT)
            return matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
        } else {
            return "general"; // Default entity if none is found
        }
    }

    /**
     * Log the message at the specified log level.
     *
     * @param level   log level
     * @param message log message
     */
    public void log(LogLevel level, String message) {
        String log = String.format(
                "[%s] [%s] %s",
                LocalTime.now(),
                level,
                message
        );

        // Log to global info logs if the level is appropriate
        if (level.ordinal() >= logLevel.ordinal()) {
            info.println(log);
            info.flush();
        }

        // Log to debug logs
        debug.println(log);
        debug.flush();

        // Log to entity-specific logs
        String entity = parseEntity(message);
        PrintWriter entityLog = getEntityLog(entity);
        if (entityLog != null) {
            entityLog.println(log);
            entityLog.flush();
        }
    }
}
