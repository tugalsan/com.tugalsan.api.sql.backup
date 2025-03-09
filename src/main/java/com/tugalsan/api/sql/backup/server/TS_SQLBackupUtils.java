package com.tugalsan.api.sql.backup.server;

import java.nio.file.*;
import java.util.*;
import com.tugalsan.api.file.server.*;
import com.tugalsan.api.file.txt.server.*;
import com.tugalsan.api.time.client.*;
import com.tugalsan.api.log.server.*;
import com.tugalsan.api.os.server.*;
import com.tugalsan.api.file.zip.server.*;
import com.tugalsan.api.function.client.maythrow.checkedexceptions.TGS_FuncMTCEUtils;
import com.tugalsan.api.sql.conn.server.*;
import com.tugalsan.api.thread.server.sync.TS_ThreadSyncTrigger;
import com.tugalsan.api.thread.server.async.scheduled.TS_ThreadAsyncScheduled;
import java.time.Duration;

public class TS_SQLBackupUtils {

    final private static TS_Log d = TS_Log.of(true, TS_SQLBackupUtils.class);
    final private static boolean USE_ZIP = false;

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

    public static void backupEveryDay(Config config, TS_SQLConnAnchor... anchors) {
        backupEveryDay(config, Arrays.asList(anchors));
    }

    public static void backupEveryDay(Config config, List<TS_SQLConnAnchor> anchors) {
        if (anchors.isEmpty()) {
            return;
        }
        TS_ThreadAsyncScheduled.everyDays(config.killTrigger.newChild(d.className), config.until, true, 1, kt -> {
            anchors.forEach(anchor -> backupNow(config, anchor));//SEQUENCIAL
        });
    }

    private static void backupNow(Config config, TS_SQLConnAnchor anchor) {
        TGS_FuncMTCEUtils.run(() -> {
            d.cr("backupEveryDay.backupNow", config.dstFolder);
//                d.ci("executeEveryDay", "waiting random time...");
//                TS_ThreadUtils.waitForSeconds(0, 60 * 60 * 2);
            var now = TGS_Time.of();
            var dstDbFolder = config.dstFolder.resolve(anchor.config.dbName);
            TS_DirectoryUtils.createDirectoriesIfNotExists(dstDbFolder);
            var pathDump = dstDbFolder.resolve(now.toString_YYYY_MM_DD() + ".dump");
            var pathZip = dstDbFolder.resolve(now.toString_YYYY_MM_DD() + ".zip");
            var pathBat = pathDump.resolveSibling(now.toString_YYYY_MM_DD() + ".bat");
            if (TS_FileUtils.isExistFile(pathBat)) {
                d.ci("backupEveryDay.backupNow", "restore already exists", pathBat.toAbsolutePath().toString());
            } else {
                d.ci("backupEveryDay.backupNow", "restore does not exists", pathBat.toAbsolutePath().toString());
                if (config.killTrigger.hasTriggered()) {
                    d.ce("backupEveryDay.backupNow", "config.killTrigger.hasTriggered()", "#1");
                    return;
                }
                d.ci("backupEveryDay.backupNow", "will run cleanup...");
                cleanUp(dstDbFolder);
                if (config.killTrigger.hasTriggered()) {
                    d.ce("backupEveryDay.backupNow", "config.killTrigger.hasTriggered()", "#2");
                    return;
                }
                d.ci("backupEveryDay.backupNow", "will run create bat...");
                restore_createBat(anchor, config.exeMYSQL, config.exe7z, pathDump, pathZip, pathBat);
                if (TS_FileUtils.isExistFile(pathZip) || TS_FileUtils.isExistFile(pathDump)) {
                    d.ci("backupEveryDay.backupNow", "backup already exists", pathZip.toAbsolutePath().toString());
                } else {
                    if (config.killTrigger.hasTriggered()) {
                        d.ce("backupEveryDay.backupNow", "config.killTrigger.hasTriggered()", "#3");
                        return;
                    }
                    d.ci("backupEveryDay.backupNow", "will run create zip...");
                    backup_createFileZip(anchor, config.exeMYSQLdump, pathDump, pathZip);
                }
                d.ci("backupEveryDay.backupNow", "backup finished.");
            }
            d.ci("backupEveryDay.backupNow", "startWait...", now.toString());
        }, e -> d.ct("backupEveryDay.backupNow", e));
    }

    //BACKUP
    private static void backup_createFileZip(TS_SQLConnAnchor anchor, Path exeMYSQLdump, Path pathDump, Path pathZip) {
        backup_toFileDump(anchor, exeMYSQLdump, pathDump);
        if (!USE_ZIP) {
            d.cr("backup_createFileZip", "skipped");
            return;
        }
        TS_FileZipUtils.zipFile(pathDump, pathZip);
        d.cr("backup_createFileZip", "zippedTo", pathZip);
        TS_FileUtils.deleteFileIfExists(pathDump);
        d.cr("backup_createFileZip", "cleanUp", pathDump);
    }

    private static void backup_toFileDump(TS_SQLConnAnchor anchor, Path exeMYSQLdump, Path pathDump) {
        d.cr("backup_toFileDump", "backupStart", pathDump);
        String cmd;
        if (anchor.config.dbPassword == null || anchor.config.dbPassword.isEmpty()) {
            cmd = exeMYSQLdump.toAbsolutePath().toString() + " -u" + anchor.config.dbUser + " -P " + anchor.config.dbPort + " --databases " + anchor.config.dbName + " -r " + pathDump.toAbsolutePath().toString();
        } else {
            cmd = exeMYSQLdump.toAbsolutePath().toString() + " -u" + anchor.config.dbUser + " -p" + anchor.config.dbPassword + " -P " + anchor.config.dbPort + " --databases " + anchor.config.dbName + " -r " + pathDump.toAbsolutePath().toString();
        }
        d.ce("backup_toFileDump", "will run cmd", cmd);
        var p = TS_OsProcess.of(cmd);
        d.cr("backup_toFileDump", "backupFinWith", "p.output", p.output);
        d.ce("backup_toFileDump", "backupFinWith", "p.error", p.error);
        if (p.exception != null) {
            d.ct("backup_toFileDump", p.exception);
        }
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
        if (USE_ZIP) {
            sj.add("\"" + exe7z.toAbsolutePath().toString() + "\" e " + pathZip.toAbsolutePath().toString());
        }
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
        if (USE_ZIP) {
            sj.add("del " + pathDump.toAbsolutePath().toString());
        }
        return sj.toString();
    }

    //CLEANUP
    private static void cleanUp(Path dstFolder) {
        var subFiles = TS_DirectoryUtils.subFiles(dstFolder, null, true, false);
        var prefix = TGS_Time.of().toString_YYYY_MM();
        subFiles.stream()
                .filter(subFile -> !subFile.getFileName().toString().startsWith(prefix))
                .forEachOrdered(subFile -> {
                    d.cr("cleanUp", "old", "deleting...", subFile);
                    TS_FileUtils.deleteFileIfExists(subFile);
                });
        if (!USE_ZIP) {
            d.cr("cleanUp", "dump", "skipped");
            return;
        }
        subFiles.stream()
                .filter(subFile -> subFile.endsWith(".dump"))
                .forEachOrdered(subFile -> {
                    d.cr("cleanUp", "dump", "deleting...", subFile);
                    TS_FileUtils.deleteFileIfExists(subFile);
                });
    }
}
/*


[spi-database] {TS_SQLBackupUtils}, {backupEveryDay.backupNow}, {[startWait...], [10.03.2025 02:22:44]}
[spi-database] {TS_SQLBackupUtils}, {backupEveryDay.backupNow}, {[C:\dat\sql\bck]}
[spi-database] {TS_SQLBackupUtils}, {backupEveryDay.backupNow}, {[restore does not exists], [C:\dat\sql\bck\autosqlweb\2025-03-10.bat]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[spi-database], [false]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[TS_ThreadAsyncBuilder0Kill], [false]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[TS_ThreadAsyncBuilderObject], [false]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[TS_ThreadAsyncAwaitSingle], [false]}
[spi-database] {TS_SQLBackupUtils}, {backupEveryDay.backupNow}, {[will run cleanup...]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[spi-database], [false]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[TS_ThreadAsyncBuilder0Kill], [false]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[TS_ThreadAsyncBuilderObject], [false]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[TS_ThreadAsyncAwaitSingle], [false]}
[spi-database] {TS_SQLBackupUtils}, {backupEveryDay.backupNow}, {[will run create bat...]}
[spi-database] {TS_SQLBackupUtils}, {restore_createBat}, {[restoreBatCreateStart], [C:\dat\sql\bck\autosqlweb\2025-03-10.bat]}
[spi-database] {TS_SQLBackupUtils}, {restore_createBat}, {[restoreBatCreateFin]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[spi-database], [false]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[TS_ThreadAsyncBuilder0Kill], [false]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[TS_ThreadAsyncBuilderObject], [false]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[TS_ThreadAsyncAwaitSingle], [false]}
[spi-database] {TS_SQLBackupUtils}, {backupEveryDay.backupNow}, {[will run create zip...]}
[spi-database] {TS_SQLBackupUtils}, {backup_toFileDump}, {[backupStart], [C:\dat\sql\bck\autosqlweb\2025-03-10.dump]}
[spi-database] {TS_SQLBackupUtils}, {backup_toFileDump}, {[will run cmd], [C:\bin\mariadb\home\bin\mysqldump.exe -uroot -ppWjXvhjhYpzPVeu33ZtIBgDkfq2hZa71 -P 3360 --databases autosqlweb -r C:\dat\sql\bck\autosqlweb\2025-03-10.dump]}
[spi-database] {TS_SQLBackupUtils}, {backup_toFileDump}, {[backupFinWith], [p.output], []}
[spi-database] {TS_SQLBackupUtils}, {backup_toFileDump}, {[backupFinWith], [p.error], []}
[spi-database] {AppServlet}, {contextInitialized}, {[#3.2]}
[spi-database] {TS_SURLExecutorList}, {add}, {[TGS_LibRqlServletUtils_CLEANTABLEBUFFER]}
[spi-database] {TS_SURLExecutorList}, {add}, {[TGS_LibRqlServletUtils_FIXDATES]}
[spi-database] {AppServlet}, {contextInitialized}, {[#3.3]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[Unnamed.started], [false]}
[spi-database] {TS_ThreadSyncTrigger}, {trigger}, {[Unnamed.started], [builder_asyncRun()[started]]}
[spi-database] {AppServlet}, {contextInitialized}, {[#3.4]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[spi-database], [false]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[TS_ThreadAsyncBuilder0Kill], [false]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[TS_ThreadAsyncBuilderObject], [false]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[TS_ThreadAsyncAwaitSingle], [false]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[TS_LibRqlBufferFastUpdateUtils], [false]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[TS_ThreadAsyncBuilder0Kill], [false]}
[spi-database] {AppServlet}, {contextInitialized}, {[#3.5]}
[spi-database] {AppServlet}, {contextInitialized}, {[#3.6]}
[spi-database] {AppServlet}, {contextInitialized}, {[PARALLEL_THRESHOLD_MB], [4096]}
[spi-database] {AppServlet}, {contextInitialized}, {[#3.7]}
[spi-database] {TS_LibBootUtils}, {_contextInitialized_runAfterExe}, {[done]}
[spi-database] {TS_LibBootUtils}, {_contextInitialized}, {[#18], [_contextInitialized_runAfterExe], [ok]}
[spi-database] {TS_ThreadSyncTrigger}, {trigger}, {[TS_ThreadAsyncAwaitSingle], [sgl_inawait_finally]}
[spi-database] {TS_ThreadSyncTrigger}, {trigger}, {[Unnamed.dead], [builder_run[dead]]}
[spi-database] {TS_SQLBackupUtils}, {backup_createFileZip}, {[zippedTo], [C:\dat\sql\bck\autosqlweb\2025-03-10.zip]}
[spi-database] {TS_SQLBackupUtils}, {backup_createFileZip}, {[cleanUp], [C:\dat\sql\bck\autosqlweb\2025-03-10.dump]}
[spi-database] {TS_SQLBackupUtils}, {backupEveryDay.backupNow}, {[backup finished.]}

[spi-database] {TS_SQLBackupUtils}, {backupEveryDay.backupNow}, {[startWait...], [10.03.2025 02:22:45]}
[spi-database] {TS_SQLBackupUtils}, {backupEveryDay.backupNow}, {[C:\dat\sql\bck]}
[spi-database] {TS_SQLBackupUtils}, {backupEveryDay.backupNow}, {[restore does not exists], [C:\dat\sql\bck\autosqlweb_rev\2025-03-10.bat]}
[spi-database] {TS_ThreadSyncTrigger}, {hasTriggered}, {[TS_ThreadAsyncAwaitSingle], [true]}
[spi-database] {TS_SQLBackupUtils}, {backupEveryDay.backupNow}, {[config.killTrigger.hasTriggered()], [#1]}
[spi-database] {TS_ThreadSyncTrigger}, {trigger}, {[TS_ThreadAsyncAwaitSingle], [sgl_inawait_finally]}
[spi-database] {TS_LibRqlBufferFastUpdateUtils}, {start}, {[lst.isEmpty()], [skip]}
[spi-database] {TS_ThreadSyncTrigger}, {trigger}, {[TS_ThreadAsyncAwaitSingle], [sgl_inawait_finally]}

*/