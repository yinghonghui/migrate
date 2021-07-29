

#include "server.h"

void migrateDataIncrementReadTarget(aeEventLoop *el, int fd, void *privdata, int mask) {
    long long start = timeInMilliseconds();
    char buf[1024];
    read(fd, buf, sizeof(buf));
    aeDeleteFileEvent(server.el, fd, AE_READABLE | AE_WRITABLE);
    aeCreateFileEvent(server.el, server.migrate_data_fd, AE_WRITABLE, migrateDataWaitTarget, NULL);
    long long end = timeInMilliseconds();
    if (end - start > 100) {
        serverLog(LL_WARNING, "migrateDataIncrementReadTarget %s cost %lld", buf, (end - start));
    }
}

void migrateDataWaitTarget(aeEventLoop *el, int fd, void *privdata, int mask) {
    sds reply;
    if (server.migrate_data_state == MIGRATE_DATA_FINISH_RDB) {
        //之前被设置为非阻塞
        serverLog(LL_WARNING, "begin accept info from target for migrate data");
        reply = sendSynchronousCommand(SYNC_CMD_READ, 300, fd, NULL);
        if (!strncmp(reply, "+FINISH", 7)) {
            server.migrate_data_state = MIGRATE_DATA_BEGIN_INCREMENT;
            serverLog(LL_WARNING, "target success to finish migrate data");
            aeDeleteFileEvent(server.el, fd, AE_READABLE | AE_WRITABLE);
            aeCreateFileEvent(server.el, server.migrate_data_fd, AE_WRITABLE, migrateDataWaitTarget, NULL);
            return;
        } else {
            serverLog(LL_WARNING, "target fail to finish migrate data %s", reply);
            aeDeleteFileEvent(server.el, fd, AE_READABLE | AE_WRITABLE);
            server.migrate_data_state = MIGRATE_DATA_FAIL_RECEIVE_ID;
            goto error;
        }
    }
    if (server.migrate_data_state == MIGRATE_DATA_BEGIN_INCREMENT) {
//        serverLog(LL_WARNING, "begin to send increment data %ld", server.migrate_data_list_buf->len);
        if (server.migrate_data_list_buf->len == 0) {
            server.startSlot = -1;
            server.endSlot = -1;
            listRelease(server.migrate_data_list_buf);
            aeDeleteFileEvent(server.el, fd, AE_WRITABLE | AE_READABLE);
            server.migrate_data_state = MIGRATE_DATA_INIT;
            server.migrate_data_end = timeInMilliseconds();
            linkClient(server.migrate_data_client);
            freeClientAsync(server.migrate_data_client);
            server.migrate_data_end = timeInMilliseconds();
            serverLog(LL_WARNING, "success to finish to send increment data cost %lld",
                      (server.migrate_data_end - server.migrate_data_begin));
            return;
        }
        sds buf = server.migrate_data_list_buf->head->value;
        if (write(fd, buf, sdslen(buf)) == -1) {
            serverLog(LL_WARNING, "fail to send increment data %s", strerror(errno));
            server.migrate_data_state = MIGRATE_DATA_FAIL_SEND_INCREMENT_DATA;
            goto error;
        } else {
            listDelNode(server.migrate_data_list_buf, server.migrate_data_list_buf->head);
            server.migrate_data_end = timeInMilliseconds();
            aeDeleteFileEvent(server.el, fd, AE_READABLE | AE_WRITABLE);
            aeCreateFileEvent(server.el, fd, AE_READABLE, migrateDataIncrementReadTarget, NULL);
//            serverLog(LL_WARNING, "success to send part increment data cost %lld",
//                      (server.migrate_data_end - server.migrate_data_begin));
            if (server.migrate_data_list_buf->len == 0) {
                server.startSlot = -1;
                server.endSlot = -1;
                listRelease(server.migrate_data_list_buf);
                aeDeleteFileEvent(server.el, fd, AE_WRITABLE | AE_READABLE);
                server.migrate_data_state = MIGRATE_DATA_INIT;
                server.migrate_data_end = timeInMilliseconds();
                linkClient(server.migrate_data_client);
                freeClientAsync(server.migrate_data_client);
                server.migrate_data_end = timeInMilliseconds();
                serverLog(LL_WARNING, "success to finish to send increment data cost %lld",
                          (server.migrate_data_end - server.migrate_data_begin));
                return;
            }
            return;
        }
    }
    error:
    server.startSlot = -1;
    server.endSlot = -1;
    listRelease(server.migrate_data_list_buf);
    aeDeleteFileEvent(server.el, fd, AE_READABLE | AE_WRITABLE);
    linkClient(server.migrate_data_client);
    freeClientAsync(server.migrate_data_client);
    return;
}


void startMigrateData(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(privdata);
    UNUSED(mask);
    sds reply;
    char *err;
    char sSlot[24];
    char eSlot[24];
    if (server.migrate_data_state == MIGRATE_DATA_BEGIN) {
        aeDeleteFileEvent(server.el, fd, AE_WRITABLE);
        // 表面目标节点准备开始发送数据
        ll2string(sSlot, sizeof(sSlot), server.startSlot);
        ll2string(eSlot, sizeof(eSlot), server.endSlot);
        err = sendSynchronousCommand(SYNC_CMD_WRITE, 300, fd, "importdata", sSlot, eSlot, NULL);
        if (err) {
            //失败了
            serverLog(LL_WARNING, "fail to notice target for migrate data by rdb");
            server.migrate_data_state = MIGRATE_DATA_FAIL_NOTICE_TARGET;
            goto error;
        } else {
            serverLog(LL_WARNING, "success to notice target to migrate data by rdb");
            server.migrate_data_state = MIGRATE_DATA_NOTICE_TARGET;
            aeCreateFileEvent(server.el, server.migrate_data_fd, AE_READABLE, startMigrateData, NULL);
            return;
        }
    }

    if (server.migrate_data_state == MIGRATE_DATA_NOTICE_TARGET) {
        reply = sendSynchronousCommand(SYNC_CMD_READ, 300, fd, NULL);
        if (!strncmp(reply, "+CONTINUE", 9)) {
            serverLog(LL_WARNING, "target able to continue migrate data by rdb");
            server.migrate_data_state = MIGRATE_DATA_BEGIN_RDB;
            rdbSaveInfo rsi, *rsiptr;
            rsiptr = rdbPopulateSaveInfo(&rsi);
            int res = migrateDataRdbSaveToTargetSockets(rsiptr, fd);
            if (res == C_ERR) {
                serverLog(LL_WARNING, "Unable to begin migrate data by rdb");
                server.migrate_data_state = MIGRATE_DATA_FAIL_START_RDB;
                goto error;
            } else {
                aeDeleteFileEvent(server.el, fd, AE_READABLE);
                serverLog(LL_WARNING, "Background migrate data by rdb");
                server.migrate_data_state = MIGRATE_DATA_SUCCESS_START_RDB;
                server.migrate_data_list_buf = listCreate();
                listSetFreeMethod(server.migrate_data_list_buf, (void (*)(void *)) sdsfree);
                return;
            }
        } else {
            serverLog(LL_WARNING, "target unable to continue migrate data by rdb: %s", reply);
            //TODO关闭
            server.migrate_data_state = MIGRATE_DATA_TARGET_NOT_INIT;
            goto error;
        }
    }
    error:
    if (reply) {
        sdsfree(reply);
    }
    if (err) {
        sdsfree(err);
    }
    server.startSlot = -1;
    server.endSlot = -1;
    aeDeleteFileEvent(server.el, fd, AE_READABLE | AE_WRITABLE);
    close(fd);
    return;
}


void migrateDataCommand(client *c) {
    if (server.migrate_data_state > MIGRATE_DATA_INIT) {
        robj *res = createObject(OBJ_STRING, sdsnew("-can not start\r\n"));
        addReply(c, res);
        decrRefCount(res);
        return;
    }
    if (server.aof_child_pid != -1 || server.rdb_child_pid != -1) {
        robj *res = createObject(OBJ_STRING, sdsnew("-can not start has child pid\r\n"));
        addReply(c, res);
        decrRefCount(res);
        return;
    }

    long long startSlot, endSlot, port;
    int fd;
    server.migrate_data_begin = timeInMilliseconds();
    robj *key = c->argv[1];
    getLongLongFromObject(c->argv[2], &port);
    getLongLongFromObject(c->argv[3], &startSlot);
    getLongLongFromObject(c->argv[4], &endSlot);
    server.startSlot = startSlot;
    server.endSlot = endSlot;
    // 连接目标节点
    fd = anetTcpNonBlockBestEffortBindConnect(NULL, key->ptr, port, NET_FIRST_BIND_ADDR);

    if (fd == -1) {
        serverLog(LL_WARNING, "Unable to connect to target to migrate data by rdb");
        server.migrate_data_state = MIGRATE_DATA_FAIL_CONNECT_TARGET;
        robj *res = createObject(OBJ_STRING, sdsnew("-Unable to connect to target\r\n"));
        addReply(c, res);
        decrRefCount(res);
        goto error;
    }
    server.migrate_data_fd = fd;
    if (aeCreateFileEvent(server.el, fd, AE_WRITABLE, startMigrateData, NULL) ==
        AE_ERR) {
        serverLog(LL_WARNING, "Can't create readable event for migrate data");
        robj *res = createObject(OBJ_STRING, sdsnew("-fail to migrate data by rdb\r\n"));
        addReply(c, res);
        decrRefCount(res);
        server.migrate_data_state = MIGRATE_DATA_FAIL_CREATE_WRITABLE_EVENT;
        goto error;
    }
    server.migrate_data_state = MIGRATE_DATA_BEGIN;
    robj *res = createObject(OBJ_STRING, sdsnew("+try to migrate data by rdb\r\n"));
    addReply(c, res);
    decrRefCount(res);
    server.migrate_data_end = timeInMilliseconds();
    return;
    error:
    if (fd != -1) {
        close(fd);
    }
    server.startSlot = -1;
    server.endSlot = -1;

}

