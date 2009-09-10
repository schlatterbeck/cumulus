/* Cumulus: Smart Filesystem Backup to Dumb Servers
 *
 * Copyright (C) 2008  The Regents of the University of California
 * Written by Michael Vrable <mvrable@cs.ucsd.edu>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

/* Backup data (segments and backup descriptors) may be stored on a remote
 * fileserver instead of locally.  The only local storage needed is for the
 * local database and some temporary space for staging files before they are
 * transferred to the remote server.
 *
 * Like encryption, remote storage is handled through the use of external
 * scripts that are called when a file is to be transferred. */

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <list>
#include <string>

#include "remote.h"
#include "store.h"
#include "util.h"

using std::string;

RemoteStore::RemoteStore(const string &stagedir, const string &script)
{
    staging_dir = stagedir;
    backup_script = script;

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
        fatal("pthread_create");
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
RemoteFile *RemoteStore::alloc_file(const string &name, const string &type)
{
    pthread_mutex_lock(&lock);
    files_outstanding++;
    pthread_mutex_unlock(&lock);
    return new RemoteFile(this, name, type, staging_dir + "/" + name);
}

/* Request that a file be transferred to the remote server.  The actual
 * transfer will happen asynchronously in another thread.  The call to enqueue
 * may block, however, if there is a backlog of data to be transferred.
 * Ownership of the RemoteFile object is transferred; the RemoteStore will be
 * responsible for its destruction. */
void RemoteStore::enqueue(RemoteFile *file)
{
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
    pthread_mutex_lock(&lock);

    while (busy)
        pthread_cond_wait(&cond, &lock);

    pthread_mutex_unlock(&lock);
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
    /* If a transfer script was specified, launch it and connect to both stdin
     * and stdout.  fd_in is stdin of the child, and fd_out is stdout for the
     * child. */
    pid_t pid = 0;
    FILE *fd_in = NULL, *fd_out = NULL;

    if (backup_script != "") {
        int fds[4];

        if (pipe(&fds[0]) < 0) {
            fatal("Unable to create pipe for upload script");
        }
        if (pipe(&fds[2]) < 0) {
            fatal("Unable to create pipe for upload script");
        }

        pid = fork();
        if (pid < 0) {
            fprintf(stderr, "Unable to fork for upload script: %m\n");
            fatal("fork: upload script");
        }

        if (pid > 0) {
            /* Parent */
            close(fds[0]);
            close(fds[3]);
            cloexec(fds[1]); fd_in = fdopen(fds[1], "w");
            cloexec(fds[2]); fd_out = fdopen(fds[2], "r");
        } else if (pid == 0) {
            /* Child */
            if (dup2(fds[0], 0) < 0)
                exit(1);
            if (dup2(fds[3], 1) < 0)
                exit(1);
            for (int i = 0; i < 3; i++)
                close(fds[i]);

            execlp("/bin/sh", "/bin/sh", "-c", backup_script.c_str(), NULL);
            fatal("exec failed");
        }
    }

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
        if (backup_script != "") {
            string cmd = "PUT ";
            cmd += uri_encode(file->type) + " ";
            cmd += uri_encode(file->remote_path) + " ";
            cmd += uri_encode(file->local_path) + "\n";

            fputs(cmd.c_str(), fd_in);
            fflush(fd_in);

            char *resp = NULL;
            size_t n;
            if (getline(&resp, &n, fd_out) < 0 || resp == NULL)
                fatal("error reading response from upload script");
            if (strchr(resp, '\n'))
                *strchr(resp, '\n') = '\0';
            if (strcmp(resp, "OK") != 0)
                fatal("error response from upload script");

            if (unlink(file->local_path.c_str()) < 0) {
                fprintf(stderr, "Warning: Deleting temporary file %s: %m\n",
                        file->local_path.c_str());
            }
        }

        delete file;
    }

    if (fd_in) fclose(fd_in);

    if (pid) {
        int status = 0;
        waitpid(pid, &status, 0);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            fprintf(stderr, "Warning: error code from upload script: %d\n",
                    status);
        }
    }

    if (fd_out) fclose(fd_out);
}

RemoteFile::RemoteFile(RemoteStore *remote,
                       const string &name, const string &type,
                       const string &local_path)
{
    remote_store = remote;
    this->type = type;
    this->local_path = local_path;
    this->remote_path = name;

    fd = open(local_path.c_str(), O_WRONLY | O_CREAT, 0666);
    if (fd < 0)
        fatal("Error opening output file");
}
