/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <atomic>
extern "C" {
#include "archivestore.h"
}
#include <logger.h>
#include <cfgparams.h>
#include <arch_core.h>
#include <data_mgmt.h>
#include <arch_store.h>
#include <arch_mem.hpp>

namespace openarchive
{
    namespace arch_store 
    {
        typedef openarchive::logger::logger logger_t; 

        logger_t *plogger = NULL;
        std::atomic<int> ref_count;

        int init_arch_store(archstore_desc_t *archstore, archstore_errno_t *err,
                            const char *log_file)
        {
            int ret=-1;

            if (!plogger) {
               
                openarchive::cfgparams::parse_config_file(); 
                std::string dir = openarchive::cfgparams::get_log_dir();
                std::string prefix = (log_file ? log_file :
                                      openarchive::cfgparams::get_log_prefix());
                long rotsize = openarchive::cfgparams::get_rotation_size();
                long freespace = openarchive::cfgparams::get_min_free_space();  
 
                plogger = new (std::nothrow) logger_t(dir, prefix, rotsize,
                                                      freespace);
                if (!plogger) {
                    *err = ENOMEM;
                }
               
                archstore->priv = NULL;
                ref_count.store(0);

            }

            if (plogger) {
                ref_count.fetch_add(1);
                ret=0;
            }

            return (ret);  
        }

        int term_arch_store(data_mgmt_t *dmptr, archstore_errno_t *err)
        {

            if (plogger) {
                plogger->flush();
            }

            ref_count.fetch_sub(1);
            if (!ref_count.load()) {  
                /*
                 * Last reference
                 */
                if (dmptr) {
                    delete dmptr;
                    dmptr = NULL;
                }
                if (plogger) { 
                    delete plogger;
                    plogger = NULL;
                }
            }

            return (0);
        }

    } /* namespace arch_store */
} /* namespace openarchive */

typedef openarchive::data_mgmt::data_mgmt data_mgmt_t;
static openarchive::arch_core::spinlock store_lock;

void arch_store_callback (arch_store_cbk_info_ptr_t ptr)
{
    app_callback_t app_cbk = ptr->get_app_cbk ();

    /*
     * Decrement the request count
     */
    ptr->decr_req ();

    if (ptr->get_done () && !ptr->get_req ()) {  
        app_cbk (ptr->get_store_desc (), ptr->get_app_callback_info_ptr (),
                 ptr->get_app_cookie (), ptr->get_ret_code (), 
                 ptr->get_err_code ());
    }

    return;
    
}

static int32_t 
init (archstore_desc_t *archstore, archstore_errno_t *archerr,
      const char *log_file)
{
    if (!archstore || !archerr) {
        assert (1==0);
    }
 
    /*
     * Lock for synchronizing access to global objects maintained 
     * in the library.
     */
    openarchive::arch_core::spinlock_handle handle (store_lock);

    int ret = openarchive::arch_store::init_arch_store(archstore, archerr,
                                                       log_file);
    if (ret) {
        return (ret);
    } 

    data_mgmt_t  * dmptr = (data_mgmt_t *) archstore->priv; 

    if (!dmptr) { 

        dmptr = new (std::nothrow) data_mgmt_t;
        archstore->priv = dmptr;

    }

    if (!dmptr) {
        *archerr = ENOMEM;
        return (-1);
    } 

    *archerr = 0;
    return (0);
}

static int32_t 
term (archstore_desc_t *archstore, archstore_errno_t *archerr)
{
    if (!archstore || !archerr) {
        assert (1==0);
    }

    data_mgmt_t *dmptr = NULL;

    openarchive::arch_core::spinlock_handle handle (store_lock);

    if (archstore) {
        dmptr = (data_mgmt_t *) archstore->priv;
    }

    return (openarchive::arch_store::term_arch_store(dmptr, archerr));
}

/*
 * Determine the list of files that have changed on the input store
 * arg1  pointer to structure containing archive store description
 * arg2  pointer to structure containing source archive store information
 * arg3  type of scan whether full/incremental
 * arg4  path to the file containing location of generated files
 * arg5  error number if any generated during the scan operation
 */

static int32_t 
scan (archstore_desc_t *archstore, archstore_info_t * store,
      archstore_scan_type_t scan_type, char * path, archstore_errno_t *archerr)
{
    data_mgmt_t *dmptr = NULL;

    if (!archstore || !archerr) {
        assert (1==0);
    }

    if (archstore) {
        dmptr = (data_mgmt_t *) archstore->priv;
    }

    std::string store_id (store->id, store->idlen);
    std::string prod (store->prod, store->prodlen);
    std::string vfs_path ("");
    openarchive::arch_loc_t loc (prod, store_id, vfs_path);

    std::string scan_path (path);
    std::error_code ec = dmptr->scan (loc, scan_type, scan_path);

    if (ec != openarchive::ok) {
        *archerr = ec.value ();
        return -1;
    }  
    
    *archerr = 0;
    return (0);
}

/*
 * Backup list of items provided in the input file
 * arg1  pointer to structure containing archive store description
 * arg2  pointer to structure containing source archive store information
 * arg3  pointer to structure containing information about files to be backed up
 * arg4  pointer to structure containing destination archive store information
 * arg5  pointer to structure containing information about files that failed 
 *       to be backed up 
 * arg6  error number if any generated during the backup operation
 * arg7  callback to be invoked after the file is backed up
 * arg8  cookie to be passed when callback is invoked
 */

static int32_t 
backup (archstore_desc_t * archstore, archstore_info_t * src_store, 
        archstore_fileinfo_t * collect_file, archstore_info_t * dest_store, 
        archstore_fileinfo_t * failed_files, archstore_errno_t * archerr, 
        app_callback_t app_cbk, void * app_ck)
{
    data_mgmt_t *dmptr = NULL;

    if (!archstore || !archerr) {
        assert (1==0);
    }

    if (archstore) {
        dmptr = (data_mgmt_t *) archstore->priv;
    }

    arch_store_cbk_info_ptr_t cbk_info = dmptr->make_shared_cbk_info ();
    if (!cbk_info) {
        *archerr = ENOMEM;
        return (-1);
    }

    /*
     * Fill the information to be consumed by the callback
     */
    cbk_info->set_store_desc    (archstore);
    cbk_info->set_src_store     (src_store);
    cbk_info->set_src_archfile  (collect_file);
    cbk_info->set_dest_store    (dest_store);
    cbk_info->set_dest_archfile (failed_files);
    cbk_info->set_app_cbk       (app_cbk);
    cbk_info->set_app_cookie    (app_ck);
    cbk_info->set_store_cbk     (arch_store_callback);

    /*
     * Fill information about product and store
     */
    std::string src_id (src_store->id, src_store->idlen);
    std::string src_prod (src_store->prod, src_store->prodlen);
    std::string path (collect_file->path, collect_file->pathlength);
    openarchive::arch_loc_t src_loc (src_prod, src_id, path, 
                                     collect_file->uuid);

    std::string dest_id (dest_store->id, dest_store->idlen);
    std::string dest_prod (dest_store->prod, dest_store->prodlen);
    std::string dest_path ("");
    openarchive::arch_loc_t dest_loc (dest_prod, dest_id, dest_path);

    std::string ff_path (failed_files->path, failed_files->pathlength);
    openarchive::arch_loc_t ff_loc;
    ff_loc.set_path (ff_path);   

    std::error_code ec = dmptr->backup_items (src_loc, dest_loc, ff_loc, 
                                              cbk_info);

    if (ec != openarchive::ok) {
        *archerr = ec.value ();
        return -1;
    }  

    *archerr = 0;
    return (0);
}

/*
 * Archive list of items provided in the input file
 * arg1  pointer to structure containing archive store description
 * arg2  pointer to structure containing source archive store information
 * arg3  pointer to structure containing information about files to be archived
 * arg4  pointer to structure containing destination archive store information
 * arg5  pointer to structure containing information about files that failed 
 *       to be archived
 * arg6  error number if any generated during the archive operation
 * arg7  callback to be invoked after the file is archived 
 * arg8  cookie to be passed when callback is invoked
 */

static int32_t 
archive (archstore_desc_t * archstore, archstore_info_t * src_store, 
         archstore_fileinfo_t * collect_file, archstore_info_t * dest_store, 
         archstore_fileinfo_t * failed_files, archstore_errno_t * archerr, 
         app_callback_t app_cbk, void * app_ck)
{
    data_mgmt_t *dmptr = NULL;

    if (!archstore || !archerr) {
        assert (1==0);
    }

    if (archstore) {
        dmptr = (data_mgmt_t *) archstore->priv;
    }

    arch_store_cbk_info_ptr_t cbk_info = dmptr->make_shared_cbk_info ();
    if (!cbk_info) {
        *archerr = ENOMEM;
        return (-1);
    }

    /*
     * Fill the information to be consumed by the callback
     */
    cbk_info->set_store_desc    (archstore);
    cbk_info->set_src_store     (src_store);
    cbk_info->set_src_archfile  (collect_file);
    cbk_info->set_dest_store    (dest_store);
    cbk_info->set_dest_archfile (failed_files);
    cbk_info->set_app_cbk       (app_cbk);
    cbk_info->set_app_cookie    (app_ck);
    cbk_info->set_store_cbk     (arch_store_callback);

    /*
     * Fill information about product and store
     */
    std::string src_id (src_store->id, src_store->idlen);
    std::string src_prod (src_store->prod, src_store->prodlen);
    std::string path (collect_file->path, collect_file->pathlength);
    openarchive::arch_loc_t src_loc (src_prod, src_id, path, 
                                     collect_file->uuid);

    std::string dest_id (dest_store->id, dest_store->idlen);
    std::string dest_prod (dest_store->prod, dest_store->prodlen);
    std::string dest_path ("");
    openarchive::arch_loc_t dest_loc (dest_prod, dest_id, dest_path);

    std::string ff_path (failed_files->path, failed_files->pathlength);
    openarchive::arch_loc_t ff_loc;
    ff_loc.set_path (ff_path);   

    std::error_code ec = dmptr->archive_items (src_loc, dest_loc, ff_loc, 
                                               cbk_info);

    if (ec != openarchive::ok) {
        *archerr = ec.value ();
        return -1;
    }  

    *archerr = 0;
    return (0);
}

/*
 * Restore a file from data management store to a destination store
 * arg1  pointer to structure containing archive store description
 * arg2  pointer to structure containing source archive store information
 * arg3  pointer to structure containing information about file to be restored
 * arg4  pointer to structure containing destination archive store information
 * arg5  pointer to structure containing information about location to which
         the file needs to be restored
 * arg6  error number if any generated during the restore operation
 * arg7  callback to be invoked after the file is restored
 * arg8  cookie to be passed when callback is invoked
 */

static int32_t 
restore (archstore_desc_t *archstore, archstore_info_t *src_store, 
         archstore_fileinfo_t *src_file, archstore_info_t *dest_store,
         archstore_fileinfo_t *dest_file, archstore_errno_t *archerr, 
         app_callback_t app_cbk, void * app_ck)
{
    data_mgmt_t *dmptr = NULL;

    if (!archstore || !archerr) {
        assert (1==0);
    }

    if (archstore) {
        dmptr = (data_mgmt_t *) archstore->priv;
    }

    arch_store_cbk_info_ptr_t cbk_info = dmptr->make_shared_cbk_info ();
    if (!cbk_info) {
        *archerr = ENOMEM;
        return (-1);
    }

    /*
     * Fill the information to be consumed by the callback
     */
    cbk_info->set_src_store     (src_store);
    cbk_info->set_src_archfile  (src_file);
    cbk_info->set_dest_store    (dest_store);
    cbk_info->set_dest_archfile (dest_file);
    cbk_info->set_app_cbk       (app_cbk);
    cbk_info->set_app_cookie    (app_ck);
    cbk_info->set_store_cbk     (arch_store_callback);

    /*
     * Get information about the product and store id from which the data 
     * needs to be read.
     */
    std::string src_id (src_store->id, src_store->idlen);
    std::string src_prod (src_store->prod, src_store->prodlen);
    std::string src_path ("");
    std::string src_uuid (src_file->path, src_file->pathlength); 
    uuid_t uid;
    uuid_parse (src_uuid.c_str(), uid);  

    std::string dest_id (dest_store->id, dest_store->idlen);
    std::string dest_prod (dest_store->prod, dest_store->prodlen);
    std::string dest_path (dest_file->path, dest_file->pathlength);

    openarchive::arch_loc_t src_loc (src_prod, src_id, src_path, uid);
    openarchive::arch_loc_t dest_loc (dest_prod, dest_id, dest_path);

    std::error_code ec = dmptr->restore_file (src_loc, dest_loc, cbk_info);
    if (ec != openarchive::ok) {
        *archerr = ec.value ();
        return -1;
    }  

    *archerr = 0;
    return (0);
}

/*
 * Read contents of a file from data management store
 * arg1  pointer to structure containing archive store description
 * arg2  pointer to structure containing source archive store information
 * arg3  pointer to structure containing information about file to be read 
 * arg4  offset in the file from which data should be read
 * arg5  buffer where the data should be read
 * arg6  number of bytes of data to be read 
 * arg7  error number if any generated during the read from file
 * arg8  callback handler to be invoked after the data is read
 * arg9  cookie to be passed when callback is invoked
 */

static int32_t 
read (archstore_desc_t *archstore, archstore_info_t *store, 
      archstore_fileinfo_t *file, off_t offset, char *buff, size_t count,
      archstore_errno_t *archerr, app_callback_t app_cbk, void *app_ck)
{
    data_mgmt_t *dmptr = NULL;

    if (!archstore || !archerr) {
        assert (1==0);
    }

    if (archstore) {
        dmptr = (data_mgmt_t *) archstore->priv;
    }

    arch_store_cbk_info_ptr_t cbk_info = dmptr->make_shared_cbk_info ();
    if (!cbk_info) {
        *archerr = ENOMEM;
        return (-1);
    }

    /*
     * Fill the information to be consumed by the callback
     */
    cbk_info->set_src_store     (store);
    cbk_info->set_src_archfile  (file);
    cbk_info->set_dest_store    (NULL);
    cbk_info->set_dest_archfile (NULL);
    cbk_info->set_app_cbk       (app_cbk);
    cbk_info->set_app_cookie    (app_ck);
    cbk_info->set_store_cbk     (arch_store_callback);

    /*
     * Get information about the product and store id from which the data 
     * needs to be read.
     */
    std::string id (store->id, store->idlen);
    std::string prod (store->prod, store->prodlen);
    std::string path (file->path, file->pathlength);

    openarchive::arch_loc_ptr_t loc = dmptr->make_shared_loc ();
    loc->set_product (prod);
    loc->set_store (id);
    loc->set_path (path);
    loc->set_uuid (file->uuid);  

    struct iovec iov;
    iov.iov_base = buff;
    iov.iov_len = count;

    std::error_code ec = dmptr->read_file (loc, offset, iov, cbk_info);
    if (ec != openarchive::ok) {
        *archerr = ec.value ();
        return -1;
    }  

    *archerr = 0;
    return (0);

}

int32_t 
get_archstore_methods (archstore_methods_t *archmethods)
{
    if (!archmethods) {
        return (-1);
    }  
 
    archmethods->init        = init;
    archmethods->fini        = term;
    archmethods->backup      = backup;
    archmethods->archive     = archive; 
    archmethods->scan        = scan;
    archmethods->restore     = restore;
    archmethods->read        = read; 
 
    return(0);
}
