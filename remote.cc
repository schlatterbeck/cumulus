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

#include <assert.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <list>
#include <string>

#include "remote.h"
#include "store.h"

using std::string;

RemoteStore::RemoteStore(const string &stagedir)
{
    staging_dir = stagedir;

    /* A background thread is created for each RemoteStore to manage the actual
     * transfers to a remote server.  The main program thread can enqueue
     * RemoteFile objects to be transferred asynchronously. */
    pthread_mutex_init(&lock, NULL);
    pthread_cond_init(&cond, NULL);
    terminate = false;
    busy = true;
    files_outstanding = 0;

    if (pthread_create(&thread, NULL, RemoteStore::start_transfer_thread,
                       (void *)this) != 0) {
        fprintf(stderr, "Cannot create remote storage thread: %m\n");
        throw IOException("pthread_create");
    }
}

/* The RemoteStore destructor will terminate the background transfer thread.
 * It will wait for all work to finish. */
RemoteStore::~RemoteStore()
{
    pthread_mutex_lock(&lock);
    terminate = true;
    pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&lock);

    if (pthread_join(thread, NULL) != 0) {
        fprintf(stderr, "Warning: Unable to join storage thread: %m\n");
    }

    assert(files_outstanding == 0);

    pthread_cond_destroy(&cond);
    pthread_mutex_destroy(&lock);
}

/* Prepare to write out a new file.  Returns a RemoteFile object.  The file
 * will initially be created in a temporary directory.  When the file is
 * written out, the RemoteFile object should be passed to RemoteStore::enqueue,
 * which will upload it to the remote server. */
RemoteFile *RemoteStore::alloc_file(const string &name)
{
    fprintf(stderr, "Allocate file: %s\n", name.c_str());
    pthread_mutex_lock(&lock);
    files_outstanding++;
    pthread_mutex_unlock(&lock);
    return new RemoteFile(this, name, staging_dir + "/" + name);
}

/* Request that a file be transferred to the remote server.  The actual
 * transfer will happen asynchronously in another thread.  The call to enqueue
 * may block, however, if there is a backlog of data to be transferred.
 * Ownership of the RemoteFile object is transferred; the RemoteStore will be
 * responsible for its destruction. */
void RemoteStore::enqueue(RemoteFile *file)
{
    fprintf(stderr, "Enqueue: %s\n", file->remote_path.c_str());

    pthread_mutex_lock(&lock);

    while (transfer_queue.size() >= MAX_QUEUE_SIZE)
        pthread_cond_wait(&cond, &lock);

    transfer_queue.push_back(file);
    files_outstanding--;
    busy = true;

    pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&lock);
}

/* Wait for all transfers to finish. */
void RemoteStore::sync()
{
    fprintf(stderr, "RemoteStore::sync() start\n");
    pthread_mutex_lock(&lock);

    while (busy)
        pthread_cond_wait(&cond, &lock);

    pthread_mutex_unlock(&lock);
    fprintf(stderr, "RemoteStore::sync() end\n");
}

void *RemoteStore::start_transfer_thread(void *arg)
{
    RemoteStore *store = static_cast<RemoteStore *>(arg);
    store->transfer_thread();
    return NULL;
}

/* Background thread for transferring backups to a remote server. */
void RemoteStore::transfer_thread()
{
    while (true) {
        RemoteFile *file = NULL;

        // Wait for a file to transfer
        pthread_mutex_lock(&lock);
        while (transfer_queue.empty() && !terminate) {
            busy = false;
            pthread_cond_broadcast(&cond);
            pthread_cond_wait(&cond, &lock);
        }
        if (terminate && transfer_queue.empty()) {
            busy = false;
            pthread_cond_broadcast(&cond);
            pthread_mutex_unlock(&lock);
            break;
        }
        busy = true;
        file = transfer_queue.front();
        transfer_queue.pop_front();
        pthread_cond_broadcast(&cond);
        pthread_mutex_unlock(&lock);

        // Transfer the file
        fprintf(stderr, "Start transfer: %s\n", file->remote_path.c_str());
        if (backup_script != "") {
            pid_t pid = fork();
            if (pid < 0) {
                fprintf(stderr, "Unable to fork for upload script: %m\n");
                throw IOException("fork: upload script");
            }
            if (pid == 0) {
                string cmd = backup_script;
                cmd += " " + file->local_path + " " + file->remote_path;
                execlp("/bin/sh", "/bin/sh", "-c", cmd.c_str(), NULL);
                throw IOException("exec failed");
            }

            int status = 0;
            waitpid(pid, &status, 0);
            if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                fprintf(stderr, "Warning: error code from upload script: %d\n",
                        status);
            }

            if (unlink(file->local_path.c_str()) < 0) {
                fprintf(stderr, "Warning: Deleting temporary file %s: %m\n",
                        file->local_path.c_str());
            }
        }
        fprintf(stderr, "Finish transfer: %s\n", file->remote_path.c_str());

        delete file;
    }
}

RemoteFile::RemoteFile(RemoteStore *remote,
                       const string &name, const string &local_path)
{
    remote_store = remote;
    this->local_path = local_path;
    this->remote_path = name;

    fd = open(local_path.c_str(), O_WRONLY | O_CREAT, 0666);
    if (fd < 0)
        throw IOException("Error opening output file");
}
