module com.tugalsan.api.sql.backup {
    requires com.tugalsan.api.time;
    requires com.tugalsan.api.log;
    requires com.tugalsan.api.os;
    requires com.tugalsan.api.thread;
    requires com.tugalsan.api.function;
    requires com.tugalsan.api.file;
    requires com.tugalsan.api.string;
    requires com.tugalsan.api.file.txt;
    requires com.tugalsan.api.file.zip;
    requires com.tugalsan.api.sql.conn;
    exports com.tugalsan.api.sql.backup.server;
}
