/* LBS: An LFS-inspired filesystem backup system
 * Copyright (C) 2006  Michael Vrable
 *
 * Backup data (segments and backup descriptors) may be stored on a remote
 * fileserver instead of locally.  The only local storage needed is for the
 * local database and some temporary space for staging files before they are
 * transferred to the remote server.
 *
 * Like encryption, remote storage is handled through the use of external
 * scripts that are called when a file is to be transferred. */

#ifndef _LBS_REMOTE_H
#define _LBS_REMOTE_H

#include <list>
#include <string>
#include <pthread.h>

class RemoteFile;

class RemoteStore {
public:
    static const size_t MAX_QUEUE_SIZE = 4;

    RemoteStore(const std::string &stagedir);
    ~RemoteStore();
    void set_script(const std::string &script)
        { backup_script = script; }
    RemoteFile *alloc_file(const std::string &name, const std::string &type);
    void enqueue(RemoteFile *file);
    void sync();

private:
    pthread_t thread;
    pthread_mutex_t lock;
    pthread_cond_t cond;

    std::string staging_dir, backup_script;
    bool terminate;             // Set when thread should shut down
    bool busy;                  // True while there are pending transfers
    std::list<RemoteFile *> transfer_queue;

    /* For error-checking purposes, track the number of files which have been
     * allocated but not yet queued to be sent.  This should be zero when the
     * RemoteStore is destroyed. */
    int files_outstanding;

    void transfer_thread();
    static void *start_transfer_thread(void *arg);
};

class RemoteFile {
public:
    /* Get the file descriptor for writing to the (staging copy of the) file.
     * The _caller_ is responsible for closing this file descriptor once all
     * data is written, and before send() is called. */
    int get_fd() const { return fd; }

    const std::string &get_local_path() const { return local_path; }

    /* Called when the file is finished--request that it be sent to the remote
     * server.  This will delete the RemoteFile object. */
    void send() { remote_store->enqueue(this); }
private:
    friend class RemoteStore;

    RemoteFile(RemoteStore *remote,
               const std::string &name, const std::string &type,
               const std::string &local_path);

    RemoteStore *remote_store;

    int fd;
    std::string type, local_path;
    std::string remote_path;
};

#endif // _LBS_REMOTE_H
