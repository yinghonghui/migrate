
#include <sys/fcntl.h>
#include "server.h"


void importDataFinishIntoDb(aeEventLoop *el, int fd, void *privdata, int mask) {
    aeDeleteFileEvent(server.el, fd, AE_READABLE);
    aeDeleteFileEvent(server.el, fd, AE_WRITABLE);

    if (server.import_data_state == IMPORT_DATA_FINISH_INTO_DB) {
        char buf[128];
        int buflen = snprintf(buf, sizeof(buf), "+FINISH\r\n");
        if (write(fd, buf, buflen) != buflen) {
            serverLog(LL_WARNING, "fail to notice source to finish ");
            linkClient(server.import_data_client);
            freeClientAsync(server.import_data_client);
            server.import_data_state = IMPORT_DATA_FAIL_SEND_RESULT;
        } else {
            serverLog(LL_WARNING, "success to notice source to finish");
            server.import_data_state = IMPORT_DATA_BEGIN_INIT;
            linkClient(server.import_data_client);
            aeCreateFileEvent(server.el, fd, AE_READABLE, readQueryFromClient, server.import_data_client);
        }
    }
    return;
}

void importDataCommand(client *c) {
    if (server.import_data_state > IMPORT_DATA_BEGIN_INIT) {
        robj *res = createObject(OBJ_STRING, sdsnew("-NOT INIT\r\n"));
        addReply(c, res);
        decrRefCount(res);
        return;
    }
    listDelNode(server.clients, c->client_list_node);
    server.import_data_client = c;

    long long startSlot, endSlot;
    char buf[128];
    getLongLongFromObject(c->argv[1], &startSlot);
    getLongLongFromObject(c->argv[2], &endSlot);
    int fd = c->fd;
    aeDeleteFileEvent(server.el, fd, AE_READABLE);
    aeDeleteFileEvent(server.el, fd, AE_WRITABLE);
    //发送响应表示接受RDB
    /* Prepare a suitable temp file for bulk transfer */
    char tmpfile[256];
    int dfd;

    snprintf(tmpfile, 256,
             "temp-%d.%ld.rdb", (int) server.unixtime, (long int) getpid());
    dfd = open(tmpfile, O_CREAT | O_WRONLY | O_EXCL, 0644);
    if (dfd == -1) {
        serverLog(LL_WARNING, "Opening the temp file needed for import data: %s",strerror(errno));
        server.import_data_state = IMPORT_DATA_FAIL_OPEN_DFD;
        goto error;
    }
    serverLog(LL_WARNING, "success open the temp file needed for import data");

    int buflen = snprintf(buf, sizeof(buf), "+CONTINUE\r\n");
    if (write(c->fd, buf, buflen) != buflen) {
        serverLog(LL_WARNING, "fail to notice source ready to import data");
        server.import_data_state = IMPORT_DATA_FAIL_SEND_CONTINUE;
        goto error;
    }

    serverLog(LL_WARNING, "begin to import rdb data from source");
    aeCreateFileEvent(server.el, fd, AE_READABLE, readSyncImportDataBulkPayload, NULL);
    server.import_data_transfer_size = -1;
    server.import_data_transfer_read = 0;
    server.import_data_transfer_last_fsync_off = 0;
    server.import_data_transfer_fd = dfd;
    server.import_data_transfer_lastio = server.unixtime;
    server.import_data_transfer_tmpfile = zstrdup(tmpfile);
    return;


    error:
    aeDeleteFileEvent(server.el, fd, AE_READABLE | AE_WRITABLE);
    if (dfd != -1) close(dfd);
    linkClient(server.import_data_client);
    freeClient(server.import_data_client);
}