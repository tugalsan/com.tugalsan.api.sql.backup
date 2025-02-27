package com.tugalsan.api.sql.backup.server;

import java.nio.file.*;
import java.util.*;
import com.tugalsan.api.file.server.*;
import com.tugalsan.api.file.txt.server.*;
import com.tugalsan.api.time.client.*;
import com.tugalsan.api.log.server.*;
import com.tugalsan.api.os.server.*;
import com.tugalsan.api.file.zip.server.*;
import com.tugalsan.api.sql.conn.server.*;
import com.tugalsan.api.thread.server.sync.TS_ThreadSyncTrigger;
import com.tugalsan.api.thread.server.async.scheduled.TS_ThreadAsyncScheduled;
import java.time.Duration;

public class TS_SQLBackupUtils {

    final private static TS_Log d = TS_Log.of(TS_SQLBackupUtils.class);

    public static String NAME_DB_PARAM_EXEMYSQLDUMP() {
        return "exeMYSQLdump";
    }

    public static String NAME_DB_PARAM_EXEMYSQL() {
        return "exeMYSQL";
    }

    public static String NAME_DB_PARAM_EXE7Z() {
        return "exe7z";
    }

    public static String NAME_DB_PARAM_APPNAMEFORBACKUPSQL() {
        return "appNameForBackupSQL";
    }

    public static record Config(TS_ThreadSyncTrigger killTrigger, Duration until, Path dstFolder, Path exeMYSQLdump, Path exeMYSQL, Path exe7z) {

    }

    public static void backupEveryDay(TS_SQLConnAnchor anchor, Config config) {
        d.cr("backupEveryDay", config.dstFolder);
        TS_ThreadAsyncScheduled.everyDays(config.killTrigger, config.until, true, 1, kt -> {
//                d.ci("executeEveryDay", "waiting random time...");
//                TS_ThreadUtils.waitForSeconds(0, 60 * 60 * 2);
            var now = TGS_Time.of();
            var dstDbFolder = config.dstFolder.resolve(anchor.config.dbName);
            TS_DirectoryUtils.createDirectoriesIfNotExists(dstDbFolder);
            var pathDump = dstDbFolder.resolve(now.toString_YYYY_MM_DD() + ".dump");
            var pathZip = dstDbFolder.resolve(now.toString_YYYY_MM_DD() + ".zip");
            var pathBat = pathDump.resolveSibling(now.toString_YYYY_MM_DD() + ".bat");
            if (TS_FileUtils.isExistFile(pathBat)) {
                d.ci("backupEveryDay", "restore already exists", pathBat.toAbsolutePath().toString());
            } else {
                d.ci("backupEveryDay", "restore does not exists", pathBat.toAbsolutePath().toString());
                if (config.killTrigger.hasTriggered()) {
                    return;
                }
                d.ci("backupEveryDay", "will run cleanup...");
                cleanUp(dstDbFolder);
                if (config.killTrigger.hasTriggered()) {
                    return;
                }
                d.ci("backupEveryDay", "will run create bat...");
                restore_createBat(anchor, config.exeMYSQL, config.exe7z, pathDump, pathZip, pathBat);
                if (TS_FileUtils.isExistFile(pathZip) || TS_FileUtils.isExistFile(pathDump)) {
                    d.ci("backupEveryDay", "backup already exists", pathZip.toAbsolutePath().toString());
                } else {
                    if (config.killTrigger.hasTriggered()) {
                        return;
                    }
                    d.ci("backupEveryDay", "will run create zip...");
                    backup_createFileZip(config.killTrigger, anchor, config.exeMYSQLdump, pathDump, pathZip);
                }
                d.ci("backupEveryDay", "backup finished.");
            }
            d.ci("backupEveryDay", "startWait...", now.toString());
        });
    }

    //BACKUP
    private static void backup_createFileZip(TS_ThreadSyncTrigger servletKillTrigger, TS_SQLConnAnchor anchor, Path exeMYSQLdump, Path pathDump, Path pathZip) {
        backup_toFileDump(anchor, exeMYSQLdump, pathDump);
        TS_FileZipUtils.zipFile(servletKillTrigger, pathDump, pathZip);
        d.cr("backup_createFileZip", "zippedTo", pathZip);
        TS_FileUtils.deleteFileIfExists(pathDump);
        d.cr("backup_createFileZip", "cleanUp", pathDump);
    }

    private static void backup_toFileDump(TS_SQLConnAnchor anchor, Path exeMYSQLdump, Path pathDump) {
        d.cr("backup_toFileDump", "backupStart", pathDump);
        String sysOut;
        if (anchor.config.dbPassword == null || anchor.config.dbPassword.isEmpty()) {
            var cmd = exeMYSQLdump.toAbsolutePath().toString() + " -u" + anchor.config.dbUser + " -P " + anchor.config.dbPort + " --databases " + anchor.config.dbName + " -r " + pathDump.toAbsolutePath().toString();
            d.ci("backup_toFileDump", "will run cmd", cmd);
            sysOut = TS_OsProcess.of(cmd).output;
        } else {
            var cmd = exeMYSQLdump.toAbsolutePath().toString() + " -u" + anchor.config.dbUser + " -p" + anchor.config.dbPassword + " -P " + anchor.config.dbPort + " --databases " + anchor.config.dbName + " -r " + pathDump.toAbsolutePath().toString();
            d.ci("backup_toFileDump", "will run cmd", cmd);
            sysOut = TS_OsProcess.of(cmd).output;
        }
        d.cr("backup_toFileDump", "backupFinWith", sysOut);
    }

    //RESTORE
    private static void restore_createBat(TS_SQLConnAnchor anchor, Path exeMYSQL, Path exe7z, Path pathDump, Path pathZip, Path pathBat) {
        d.cr("restore_createBat", "restoreBatCreateStart", pathBat.toAbsolutePath().toString());
        TS_FileTxtUtils.toFile(restore_createBatContent(pathDump, exeMYSQL, exe7z, pathZip, anchor), pathBat, false);
        d.cr("restore_createBat", "restoreBatCreateFin");
    }

    //TODO  --host=remote.example.com --port=13306
    private static String restore_createBatContent(Path pathDump, Path exeMYSQL, Path exe7z, Path pathZip, TS_SQLConnAnchor anchor) {
        var sj = new StringJoiner("\n");
        sj.add("\"" + exe7z.toAbsolutePath().toString() + "\" e " + pathZip.toAbsolutePath().toString());
        var o = " --host=" + anchor.config.dbIp + " --port=" + anchor.config.dbPort;
        if (anchor.config.dbPassword == null || anchor.config.dbPassword.isEmpty()) {
            sj.add(exeMYSQL.toAbsolutePath().toString() + o + " -u " + anchor.config.dbUser + " -P " + anchor.config.dbPort + " -e \"DROP DATABASE IF EXISTS " + anchor.config.dbName + ";\"");
            sj.add(exeMYSQL.toAbsolutePath().toString() + o + " -u " + anchor.config.dbUser + " -P " + anchor.config.dbPort + " -e \"CREATE DATABASE " + anchor.config.dbName + ";\"");
            sj.add(exeMYSQL.toAbsolutePath().toString() + o + " -u" + anchor.config.dbUser + " -P " + anchor.config.dbPort + " " + anchor.config.dbName + " < " + pathDump.toAbsolutePath().toString());
        } else {
            sj.add(exeMYSQL.toAbsolutePath().toString() + o + " -u " + anchor.config.dbUser + " -p" + anchor.config.dbPassword + " -P " + anchor.config.dbPort + " -e \"DROP DATABASE IF EXISTS " + anchor.config.dbName + ";\"");
            sj.add(exeMYSQL.toAbsolutePath().toString() + o + " -u " + anchor.config.dbUser + " -p" + anchor.config.dbPassword + " -P " + anchor.config.dbPort + " -e \"CREATE DATABASE " + anchor.config.dbName + ";\"");
            sj.add(exeMYSQL.toAbsolutePath().toString() + o + " -u" + anchor.config.dbUser + " -p" + anchor.config.dbPassword + " -P " + anchor.config.dbPort + " " + anchor.config.dbName + " < " + pathDump.toAbsolutePath().toString());
        }
        sj.add("del " + pathDump.toAbsolutePath().toString());
        return sj.toString();
    }

    //CLEANUP
    private static void cleanUp(Path dstFolder) {
        var subFiles = TS_DirectoryUtils.subFiles(dstFolder, null, true, false);
        var prefix = TGS_Time.of().toString_YYYY_MM();
        subFiles.stream()
                .filter(subFile -> !subFile.getFileName().toString().startsWith(prefix))
                .forEachOrdered(subFile -> {
                    d.cr("cleanUp", "old", subFile, "deleting...");
                    TS_FileUtils.deleteFileIfExists(subFile);
                });
        subFiles.stream()
                .filter(subFile -> subFile.endsWith(".dump"))
                .forEachOrdered(subFile -> {
                    d.cr("cleanUp", "dump", subFile, "deleting...");
                    TS_FileUtils.deleteFileIfExists(subFile);
                });
    }
}
