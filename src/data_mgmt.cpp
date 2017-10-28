/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <data_mgmt.h>
#include <arch_tls.h>

namespace openarchive
{
    namespace data_mgmt
    {
        const uint64_t default_extent_size = 0x400000; /* default 4MB extent */
        const uint32_t default_extent_bits = 22; /* 4MB == 22bits width      */
        const uint32_t meta_cache_ttl = 864000; /* ttl for memcached entries */
        const uint32_t fd_cache_size = 64;  /* number of entries in fd cache */

        data_mgmt::data_mgmt (void): ready (false), 
                                     extent_size (default_extent_size),
                                     num_bits (default_extent_bits),
                                     cbk_pool ("cbkpool"), 
                                     file_pool ("filepool"),
                                     req_pool ("reqpool"),
                                     dmstat_pool ("dmstatpool"),
                                     fileattr_pool ("fileattrpool"),
                                     loc_pool ("locpool"),
                                     done (false) 
        {
            log_level = openarchive::cfgparams::get_log_level();

            engine = openarchive::arch_engine::alloc_engine ();
            if (!engine) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to allocate engine ";
                return;
            }

            /*
             *  Start the memory profiler thread
             */
            memprof_th = new std::thread (&data_mgmt::mem_prof, this);
            if (!memprof_th) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to create memory profiler thread ";
                return;
            } 

            ready = true;
        }

        data_mgmt::~data_mgmt (void)
        {

            /*
             * Stop the memory profiler thread
             */ 
            done.store (true);
            if (memprof_th) {
                memprof_th->join ();
                delete memprof_th;
            }

            if (engine) {
                engine->stop ();
            }    

            /*
             * Release the parent links in the iopx tree
             */
            if (source) {
                source->reset_links ();
            }

            if (sink) {
                sink->reset_links ();
            }
        }

        std::error_code data_mgmt::set_extent_size (uint64_t size)
        {
            /*
             * If there are active sources/sinks then do not change the size.
             */
            if (source || sink) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV(log, openarchive::logger::level_error)
                              << " failed to set extent size"
                              << " as there are active source/sink.";
                std::error_code ec(EPERM, std::generic_category());
                return(ec);
            }

            /*
             * The extent size should satisfy the following checks:
             * 1) Multiple of 4MB
             */
            if (size % default_extent_size) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " invalid extent size" << size;
                std::error_code ec (EINVAL, std::generic_category());
                return (ec); 
            }
            
            extent_size=size;
            num_bits=openarchive::arch_core::bitops::bit_width (extent_size);

            return (openarchive::success); 
            
        }

        std::error_code data_mgmt::scan (arch_loc_t &loc, 
                                         archstore_scan_type_t scan_type,
                                         std::string &path)
        {
            /*
             * This is the method that should be invoked by external 
             * applications for determing the files to be backed from a 
             * store.
             */

            iopx_tree_cfg_t src_cfg = {
                                          loc.get_product (),
                                          loc.get_store (),
                                          std::string ("source"),
                                          true,
                                          false,
                                          0,
                                          false,
                                          0
                                      }; 

            alloc_src_iopx (src_cfg);

            if (!source) {

                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to allocate source iopx tree";
                std::error_code ec (ENOMEM, std::generic_category ());
                return (ec); 
            }
            
            /*
             * We will use fast ioservice for scheduling source and 
             * sink iopx fops.
             */ 

            file_ptr_t    fp      = file_pool.make_shared ();
            req_ptr_t     req     = req_pool.make_shared ();

            fp->set_loc (loc);
            fp->set_iopx (source); 

            std::string file_path = path;

            init_scan_req (fp, req, &file_path, scan_type);

            std::error_code ec = source->scan (fp, req);
            if (ec != ok) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " scan failed for "  << loc.get_product ()
                               << " : " << loc.get_store () 
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);
                return(ec);
            }

            return (openarchive::success);
        }

        std::error_code data_mgmt::backup_items (arch_loc_t & src, 
                                                 arch_loc_t & dest,
                                                 arch_loc_t & fail,
                                                 arch_store_cbk_info_ptr_t cbki)
        {
            return process_items (BACKUP, true, src, dest, fail,  
                                  &data_mgmt::backup_worker, cbki);
        }

        std::error_code data_mgmt::archive_items (arch_loc_t & src, 
                                                  arch_loc_t & dest,
                                                  arch_loc_t & fail,
                                                  arch_store_cbk_info_ptr_t cbk)
        {
            return process_items (ARCHIVE, true, src, dest, fail,  
                                  &data_mgmt::archive_worker, cbk);
        }

        std::error_code data_mgmt::process_items (arch_op_type op_type,
                                                  bool is_fast_iosvc,
                                                  arch_loc_t & src, 
                                                  arch_loc_t & dest, 
                                                  arch_loc_t & fail,
                                                  data_mgmt_worker fptr,
                                                  arch_store_cbk_info_ptr_t cbki)
        {
            /*
             * Process the items mentioned in src location.
             */
            src_iosvc = engine->get_ioservice (is_fast_iosvc);

            std::ifstream inpstream;
            std::string collectfile = src.get_pathstr ();
            inpstream.open (collectfile);

            if (!inpstream.is_open ()) {
                /*
                 * Failed to open the file containing list of items to be
                 * processed.
                 */  
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to open " << collectfile;
                std::error_code ec (errno, std::generic_category());
                return (ec); 
            }

            /*
             * Allocate tracker for keeping track of failed files.
             */ 
            file_tracker_ptr_t fftracker = boost::make_shared <file_tracker_t> (fail.get_pathstr());
            if (NULL == fftracker.get() || false==fftracker->good()) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to allocate failed file tracker "
                               << fail.get_pathstr();
                std::error_code ec (ENOMEM, std::generic_category());
                return (ec); 
            } 

            /*
             * The number of items that are part of the file is determined by
             * parsing the file name.
             * The file name will be in the following format:
             * name.count 
             */
            size_t pos = collectfile.find_last_of (".");
            std::string scount = collectfile.substr (pos+1);
            uint64_t count = boost::lexical_cast<uint64_t> (scount);

            /*
             * Get the number of work items that need to be created.
             */
            uint64_t work_items = openarchive::cfgparams::get_num_work_items (op_type);
            uint64_t work_item_size = count/work_items; 
            if (!work_item_size) {
                work_item_size = 1; 
            }

            uint64_t batch_size = 0;
            uint64_t batch_count = 1;

            std::list <std::string> work_items_list;

            std::string batch_name;
            std::ofstream batch_writer;

            while(inpstream.good ()) {

                if (!batch_size) {
                    /*
                     * A new batch of files has started. Open the file.
                     */ 
                    batch_name = collectfile + std::string (".") + 
                                 boost::lexical_cast<std::string> (batch_count);

                    if (batch_writer.is_open ()) {
                        batch_writer.close ();
                    }

                    batch_writer.open (batch_name);
                    if (!batch_writer.is_open ()) {
                        BOOST_LOG_FUNCTION ();
                        BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                       << " failed to allocate batch " 
                                       << batch_name
                                       << fail.get_pathstr();
                        std::error_code ec (EBADFD, std::generic_category());
                        return (ec); 
                    }  
                }

                std::string file_path ("");
                inpstream >>std::skipws >>file_path; 
                if (file_path.length ()) {
                    batch_writer << file_path << std::endl;  
                    batch_size++;
                }

                if (batch_size == work_item_size) {
                    /*
                     * The current batch is full. Reset batch_size so that
                     * new batch can be created in the next iteration.
                     */
                    batch_size = 0;
                    batch_count++;
                    batch_writer.close ();
                    work_items_list.push_back (batch_name);
                }
            }

            if (batch_size) {
                /*
                 * The current batch has some leftovers. 
                 */
                batch_writer.close ();
                work_items_list.push_back (batch_name);
            }

            /*
             * The list of work items is ready. Start assigning work to 
             * worker threads. After all the work items have been processed
             * we need to rename the collect file and let it remain there
             * for debugging purposes.
             */ 
            dmstats_ptr_t dmp = dmstat_pool.make_shared ();
            std::list<std::string>::iterator iter;

            for(iter = work_items_list.begin (); 
                iter != work_items_list.end (); iter++) {

                dmp->incr_pending (1);
                src_iosvc->post (boost::bind (fptr, this, src, dest, *iter, dmp,
                                              fftracker, cbki));
            }
             
            dmp->set_done ();

            /*
             * Rename the collect file for debugging purposes.
             */
            std::string save_path = collectfile+".save"; 
            if (::rename (collectfile.c_str (), save_path.c_str())) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to rename file  from " 
                               << collectfile << " to " << save_path 
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);
            } 
                
            return (openarchive::success); 

        }

        std::error_code data_mgmt::backup_worker (arch_loc_t &src_loc,
                                                  arch_loc_t &dest_loc,
                                                  std::string collectfile,
                                                  dmstats_ptr_t dmp, 
                                                  file_tracker_ptr_t fftracker,
                                                  arch_store_cbk_info_ptr_t cbk)
        {
            /*
             * Backup the list of files mentioned in src location.
             */
            static iopx_tree_cfg_t src_cfg = {
                                                 src_loc.get_product (),
                                                 src_loc.get_store (),
                                                 std::string ("source"),
                                                 true,
                                                 false,
                                                 0,
                                                 false,
                                                 0
                                             }; 

            alloc_src_iopx (src_cfg);

            if (!source) {

                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to allocate source iopx tree";
                std::error_code ec (ENOMEM, std::generic_category ());
                return (ec); 
            }
            
            static iopx_tree_cfg_t sink_cfg = {
                                                  dest_loc.get_product (),
                                                  dest_loc.get_store (),
                                                  std::string ("sink"),
                                                  true,
                                                  false,
                                                  0,
                                                  false,
                                                  0
                                              }; 

            alloc_sink_iopx (sink_cfg);

            if (!sink) {

                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to allocate sink iopx tree";
                std::error_code ec (ENOMEM, std::generic_category());
                return (ec); 
            }

            /*
             * Start backing up entries in the collect file one after 
             * the other.
             */
 
            std::ifstream inpstream;
            inpstream.open (collectfile);

            if (!inpstream.is_open ()) {
                /*
                 * Failed to open the file containing list of files to be
                 * backed up.
                 */  
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to open files list";
                work_done_cbk (cbk, dmp, -1, errno); 
                return std::error_code (errno, std::generic_category ());
            }

            tls_ref_t tls_ref = openarchive::arch_tls::get_arch_tls ();
            file_ptr_t fp = tls_ref->alloc_arch_file ();
            fp->set_iopx (source); 

            req_ptr_t req = tls_ref->alloc_iopx_req ();

            /*
             * Allocate a buffer big enough to service the read requests.
             */
            malloc_intfx_ptr_t mptr = openarchive::arch_mem::get_malloc_intfx();
            if (!mptr) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to allocate a malloc_intfx object"
                               << " while processing " << collectfile;
                work_done_cbk (cbk, dmp, -1, ENOMEM); 
                return std::error_code (ENOMEM, std::generic_category ());
            }

            buff_ptr_t bufp = mptr->posix_memalign (mptr->get_page_size(),
                                                    extent_size);

            if (!(bufp->get_base())) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to allocate buffer"
                               << " while processing " << collectfile;
                work_done_cbk (cbk, dmp, -1, ENOMEM); 
                return std::error_code (ENOMEM, std::generic_category ());
            }

            while(inpstream.good ()) {

                std::string file_path;
                inpstream >>std::skipws >>file_path; 

                /*
                 * Check whether the specified path is a file. If so
                 * back it up.
                 */ 

                openarchive::arch_loc::arch_loc loc;
                loc.set_path (file_path);
                loc.set_product (src_loc.get_product ());
                loc.set_store (src_loc.get_store ());

                fp->set_loc (loc);

                /*
                 * Extract the uuid for the file.
                 */
                struct iovec iov;
                iov.iov_base = loc.get_uuid_ptr ();
                iov.iov_len = sizeof (uuid_t);

                init_getuuid_req (fp, req, &iov);

                std::error_code ec = source->getuuid (fp, req);
                if (ec != ok) {

                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " failed to get uuid " 
                                   << " for " << file_path
                                   << " error code: " << ec.value() 
                                   << " error desc: " << ec.message();
                    fftracker->append (file_path);
                    continue;  
                }

                struct stat statbuff = {0,};
                init_stat_req (fp, req, &statbuff);

                ec = source->stat (fp, req);
                if (ec != ok) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " stat failed for file " 
                                   << file_path
                                   << " error code: " << errno
                                   << " error desc: " << strerror(errno);

                    fftracker->append (file_path);
                    continue;  
                }
 
                if (S_ISREG(statbuff.st_mode)) {
                    /*
                     * This is a regular file. A candidate for being backed
                     * up. Check whether extent based backups are enabled.
                     * The collect file is supposed to contain the individual
                     * extents to be backed up. 
                     */

                    size_t fsize = statbuff.st_size;
                    if (openarchive::cfgparams::extent_based_backups(src_loc)) {
                        fsize = (fsize > extent_size ? extent_size : fsize); 
                    } 
                    
                    ec = backup_file (loc, dest_loc, fsize, bufp, 
                                      statbuff.st_size);
                    if (ec != ok) {
                        BOOST_LOG_FUNCTION ();
                        BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                       << " failed to backup file " 
                                       << file_path;

                        fftracker->append (file_path);
                    }
                } 
            }

            work_done_cbk (cbk, dmp, 0, 0); 

            /*
             * The collect file corresponding to this work item has been 
             * processed. Unlink it.
             */
            if (inpstream.is_open ()) {
                inpstream.close (); 
            } 

            if (::unlink (collectfile.c_str ())) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to unlink file " 
                               << collectfile 
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);
            } 
  
            return (openarchive::success); 
        }

        std::error_code data_mgmt::backup_file (arch_loc_t &src_loc,
                                                arch_loc_t &dest_loc,
                                                size_t file_size,
                                                buff_ptr_t bufp,
                                                size_t actual_file_size)
        {
            /*
             * Backup typically translates to reading from source iopx and 
             * writing to sink iopx.
             */
            tls_ref_t tls_ref = openarchive::arch_tls::get_arch_tls ();

            file_ptr_t src_fp = tls_ref->alloc_arch_file ();
            src_fp->set_loc (src_loc);  
            src_fp->set_iopx (source); 

            if (log_level >= openarchive::logger::level_debug_2) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                               << " will start backing up file "
                               << src_loc.get_pathstr();
            }

            req_ptr_t req = tls_ref->alloc_iopx_req ();
            openarchive::iopx_req::init_open_req (src_fp, req, 
                                                  O_RDONLY | O_NOATIME);

            std::error_code ec = source->open (src_fp, req);
            if (ec != ok) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " open failed for file " 
                               << src_loc.get_pathstr()
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);
                return(ec);
            }

            uuid_t uuid;
            src_loc.get_uuid (uuid);

            openarchive::arch_loc::arch_loc loc;
            loc.set_path (src_loc.get_pathstr());
            loc.set_product (dest_loc.get_product ());
            loc.set_store (dest_loc.get_store ());
            loc.set_uuid (uuid);

            file_ptr_t sink_fp = tls_ref->alloc_arch_file ();
            sink_fp->set_loc (loc);  
            sink_fp->set_iopx (sink); 

            openarchive::iopx_req::init_creat_req (sink_fp, req, 
                                                   O_WRONLY | O_NOATIME,
                                                   0640);

            /*
             * For some of the data management products it's imperative to 
             * specify the actual file size during file creation itself.
             */ 
            req->set_len (actual_file_size);           
            ec = sink->open (sink_fp, req);
            if (ec != ok) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " open failed for file " 
                               << loc.get_pathstr ()
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);
                return(ec);
            }

            /*
             * After the file has been successfully opened on the destination
             * the UUID aasociated with the file on destination store will be
             * set in the info field of the req. Get the UUID.
             */ 
            uuid_parse (req->get_info().c_str(), uuid);

            off_t offset = 0;
            size_t buffsize = bufp->get_size ();
            size_t remaining = file_size;

            while(remaining) {

                size_t bytes = ((remaining > buffsize)? buffsize : remaining); 
                size_t sent;

                std::error_code ec = sendfile (src_fp, sink_fp, offset, bufp,
                                               bytes, sent);

                if (ec != ok) {
                    return ec; 
                }

                offset += sent;
                remaining -= sent;
            }

            /*
             * The file has been backed up successfully. Set the extended 
             * attributes to indicate that the file has been backed up.
             */

            file_attr_ptr_t fattr = tls_ref->get_fattr ();
            fattr->set_product (dest_loc.get_product ());
            fattr->set_store (dest_loc.get_store ());
            fattr->set_uuid (uuid);

            ec = fsetxattrs(source, src_fp, fattr, true, false);
            if (ec != ok) {

                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to set xattrs for " 
                               << src_loc.get_pathstr () << " : "
                               << " error code: " << ec.value() 
                               << " error desc: " << ec.message();
                return ec;

            }

            if (log_level >= openarchive::logger::level_debug_2) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                               << " successfully backed up file "
                               << src_loc.get_pathstr();
            }

            return (openarchive::success); 
        } 

        std::error_code data_mgmt::archive_worker (arch_loc_t &src_loc,
                                                   arch_loc_t &dest_loc,
                                                   std::string collectfile,
                                                   dmstats_ptr_t dmp, 
                                                   file_tracker_ptr_t fftracker,
                                                   arch_store_cbk_info_ptr_t cbk)
        {
            /*
             * Archive the list of files mentioned in src location.
             */
            static iopx_tree_cfg_t src_cfg = {
                                                 src_loc.get_product (),
                                                 src_loc.get_store (),
                                                 std::string ("source"),
                                                 true,
                                                 false,
                                                 0,
                                                 false,
                                                 0
                                             }; 

            alloc_src_iopx (src_cfg);

            if (!source) {

                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to allocate source iopx tree";
                std::error_code ec (ENOMEM, std::generic_category ());
                return (ec); 
            }
            
            /*
             * Start archiving entries in the collect file one after 
             * the other.
             */
 
            std::ifstream inpstream;
            inpstream.open (collectfile);

            if (!inpstream.is_open ()) {
                /*
                 * Failed to open the file containing list of files to be
                 * backed up.
                 */  
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to open files list";
                work_done_cbk (cbk, dmp, -1, errno); 
                return std::error_code (errno, std::generic_category ());
            }

            tls_ref_t tls_ref = openarchive::arch_tls::get_arch_tls ();
            file_ptr_t fp = tls_ref->alloc_arch_file ();
            fp->set_iopx (source); 

            req_ptr_t req = tls_ref->alloc_iopx_req ();

            while(inpstream.good ()) {

                std::string file_path;
                inpstream >>std::skipws >>file_path; 

                /*
                 * Check whether the specified path is a file. If so
                 * archive it.
                 */ 

                openarchive::arch_loc::arch_loc loc;
                loc.set_path (file_path);
                loc.set_product (src_loc.get_product ());
                loc.set_store (src_loc.get_store ());
                fp->set_loc (loc);

                struct stat statbuff = {0,};
                init_stat_req (fp, req, &statbuff);

                std::error_code ec = source->stat (fp, req);
                if (ec != ok) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " stat failed for file " 
                                   << file_path
                                   << " error code: " << errno
                                   << " error desc: " << strerror(errno);

                    fftracker->append (file_path);
                    continue;  
                }
 
                if (S_ISREG(statbuff.st_mode)) {

                    /*
                     * This is a regular file. A candidate for being archived.
                     */
                    std::list <arch_loc_t> tgt_list;
                    init_resolve_req (fp, req, &tgt_list, statbuff.st_size); 

                    ec = source->resolve (fp, req);
                    if (ec != ok) {
                        BOOST_LOG_FUNCTION ();
                        BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                       << " failed to get path for file " 
                                       << fp->get_loc().get_pathstr()
                                       << " error code: " << errno
                                       << " error desc: " << strerror(errno);
                        fftracker->append (file_path);
                        continue;  
                    }

                    /*
                     * We have the list of physical paths to be archived. Start 
                     * archiving each of the physical paths.
                     */
                    std::list<arch_loc_t>::iterator iter;
                    for(iter = tgt_list.begin (); iter != tgt_list.end (); iter++) {

                        ec = archive_file (*iter);
                        if (ec != ok) {
                            BOOST_LOG_FUNCTION ();
                            BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                           << " failed to archive file " 
                                           << (*iter).get_pathstr()
                                           << " error code: " << errno
                                           << " error desc: " << strerror(errno);

                            fftracker->append (file_path);
                        }
                    }
                } 
            }

            work_done_cbk (cbk, dmp, 0, 0); 

            /*
             * The collect file corresponding to this work item has been 
             * processed. Unlink it.
             */
            if (inpstream.is_open ()) {
                inpstream.close (); 
            } 

            if (::unlink (collectfile.c_str ())) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to unlink file " 
                               << collectfile 
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);
            } 
  
            return (openarchive::success); 
        }

        std::error_code data_mgmt::archive_file (arch_loc_t & src)
        {

            /*
             * We will open the file in O_RDWR mode since we will be 
             * archiving it.
             */ 
            tls_ref_t tls_ref = openarchive::arch_tls::get_arch_tls ();
            file_ptr_t fp = tls_ref->alloc_arch_file ();
            fp->set_loc (src);
            fp->set_iopx (source); 
            req_ptr_t req = tls_ref->alloc_iopx_req ();
            file_attr_ptr_t fattr = tls_ref->get_fattr ();

            /*
             * Set the iopx tree which will be opertaing on the file. This is
             * a must if the close needs to be invoked automatically for each 
             * of the iopx in the tree when the file object is destroyed.
             * This field need not be set if the consumer of the file object
             * feels that close on the file will be invoked in all the 
             * circumstances, which is difficlut to ensure and maintain. 
             * Setting iopx tree will ensure that close will be invoked on
             * all the iopx in the tree when the file object is destroyed.  
             */
            fp->set_iopx (source); 

            openarchive::iopx_req::init_open_req (fp, req, O_RDWR | O_NOATIME);

            std::error_code ec = source->open (fp, req);
            if (ec != ok) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " open failed for file " 
                               << fp->get_loc().get_pathstr()
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);
                return(ec);
            }

            /*
             * We will extract file size using extended attributes.
             * If this is successful then it means that the file was
             * already archived. We will fail in this case.    
             */
            uint64_t fsize; 
            ec = get_file_size (source, fp, req, fsize);
            if (ec == ok) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " file seems to be already archived " 
                               << fp->get_loc().get_pathstr()
                               << " size: " << fsize;

                return (openarchive::success); 
             
            }

            struct stat statbuff = {0,};
            openarchive::iopx_req::init_fstat_req (fp, req, &statbuff);

            ec = source->fstat (fp, req);
            if (ec != ok) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " fstat failed for file " 
                               << fp->get_loc().get_pathstr()
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);
                return(ec);
            }

            /*
             * Save the file attributes. These attributes contain metadata
             * about file location and size. The metadata will then be applied 
             * as extended file system attributes after the file is archived.
             */
            fattr->set_fsize (statbuff.st_size);
            fattr->set_blksize (statbuff.st_blksize);
            fattr->set_blocks (statbuff.st_blocks);
       
            /*
             * Now that the open has suceeded and we have the size of the 
             * file to be archived we will archive the file.
             */
            ec = fsetxattrs(source, fp, fattr, false, true);
            if (ec != ok) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to set xattrs for " 
                               << fp->get_loc ().get_pathstr () << " : "
                               << fp->get_loc ().get_uuidstr ()
                               << " error code: " << ec.value() 
                               << " error desc: " << ec.message();
                return(ec);

            }

            ec = ftruncate (source, fp, req, 0);
            if (ec != ok) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to truncate " 
                               << fp->get_loc ().get_pathstr () << " : "
                               << fp->get_loc ().get_uuidstr ()
                               << " error code: " << ec.value() 
                               << " error desc: " << ec.message();
                return(ec);

            }

            return (openarchive::success); 
        }

        std::error_code data_mgmt::restore_file (arch_loc_t & src, 
                                                 arch_loc_t & dest, 
                                                 arch_store_cbk_info_ptr_t cbki)
        {
            /*
             * This is the method that should be invoked by external 
             * applications for recovering a file. The data would be
             * recovered from archive store using source iopx tree and 
             * would be written using sink iopx tree. Both source and 
             * sink trees are required. Check and allocate source and 
             * sink iopx trees.
             */

            static iopx_tree_cfg_t src_cfg =  {
                                                  src.get_product (),
                                                  src.get_store (),
                                                  std::string ("source"),
                                                  true,
                                                  true,
                                                  meta_cache_ttl,
                                                  false,
                                                  0
                                              }; 

            alloc_src_iopx (src_cfg);

            if (!source) {

                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to allocate source iopx tree";
                std::error_code ec (ENOMEM, std::generic_category());
                return (ec); 
            }

            /*
             * We will use fast ioservice for scheduling source and 
             * sink iopx fops.
             */ 
            src_iosvc = engine->get_ioservice (true);

            static iopx_tree_cfg_t sink_cfg = {
                                                  dest.get_product (),
                                                  dest.get_store (),
                                                  std::string ("sink"),
                                                  true,
                                                  false,
                                                  0,
                                                  false,
                                                  0
                                              };  

            alloc_sink_iopx (sink_cfg);

            if (!sink) {

                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to allocate sink iopx tree";
                std::error_code ec (ENOMEM, std::generic_category());
                return (ec); 
            }

            dmstats_ptr_t dmp = dmstat_pool.make_shared ();
            dmp->incr_pending (1);
            src_iosvc->post(boost::bind(&data_mgmt::restore_worker, this,
                                        src, dest, dmp,  cbki));
            dmp->set_done ();
            return (openarchive::success);
        } 

        std::error_code data_mgmt::restore_worker (arch_loc_t &loc,
                                                   arch_loc_t &dest_loc,
                                                   dmstats_ptr_t dmp, 
                                                   arch_store_cbk_info_ptr_t cbk)
        {
            tls_ref_t tls_ref = openarchive::arch_tls::get_arch_tls ();
            file_ptr_t src_fp = tls_ref->alloc_arch_file ();
            src_fp->set_loc (loc);  
            src_fp->set_iopx (source); 

            req_ptr_t req = tls_ref->alloc_iopx_req ();

            /*
             * Allocate a buffer big enough to service the read requests.
             */
            malloc_intfx_ptr_t mptr = openarchive::arch_mem::get_malloc_intfx();
            if (!mptr) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to allocate a malloc_intfx object"
                               << " while processing " << loc.get_pathstr();
                work_done_cbk (cbk, dmp, -1, ENOMEM); 
                return std::error_code (ENOMEM, std::generic_category ());
            }

            buff_ptr_t bufp = mptr->posix_memalign (mptr->get_page_size(),
                                                    extent_size);

            if (!(bufp->get_base())) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to allocate buffer"
                               << " while processing " << loc.get_pathstr();
                work_done_cbk (cbk, dmp, -1, ENOMEM); 
                return std::error_code (ENOMEM, std::generic_category ());
            }

            /*
             * Open the files on source and sink stores.
             */
            openarchive::iopx_req::init_open_req (src_fp, req, 
                                                  O_RDONLY | O_NOATIME);

            std::error_code ec = source->open (src_fp, req);
            if (ec != ok) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " open failed for file " 
                               << loc.get_pathstr()
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);
                work_done_cbk (cbk, dmp, -1, ENOMEM); 
                return(ec);
            }

            file_ptr_t sink_fp = tls_ref->alloc_arch_file ();
            sink_fp->set_loc (dest_loc);  
            sink_fp->set_iopx (sink); 

            openarchive::iopx_req::init_creat_req (sink_fp, req, 
                                                   O_WRONLY | O_NOATIME,
                                                   0640); 

            ec = sink->open (sink_fp, req);
            if (ec != ok) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " open failed for file " 
                               << loc.get_pathstr ()
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);
                work_done_cbk (cbk, dmp, -1, ENOMEM); 
                return(ec);
            }

            off_t offset = 0;
            size_t buffsize = bufp->get_size ();
            size_t sent = 0;

            do {

                ec = sendfile (src_fp, sink_fp, offset, bufp,
                               buffsize, sent);

                if (ec != ok) {
                    work_done_cbk (cbk, dmp, -1, ENOMEM); 
                    return ec; 
                }

                offset += sent;

            } while (sent > 0);

            /*
             * The file has been restored successfully.
             */
            work_done_cbk (cbk, dmp, 0, 0); 

            if (log_level >= openarchive::logger::level_error) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " successfully restored file "
                               << loc.get_pathstr();
            }

            return (openarchive::success); 

        }

        std::error_code data_mgmt::read_file (arch_loc_ptr_t loc, 
                                              uint64_t offset,
                                              const struct iovec iov, 
                                              arch_store_cbk_info_ptr_t cbki)
        {
            /*
             * This is the method that should be invoked by external 
             * applications for reading data from a file in archive store.
             * The data would be read from archive store using source 
             * iopx tree. 
             */

            if (log_level >= openarchive::logger::level_debug_2) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                               << " received request for file " 
                               << loc->get_uuidstr ()
                               << " file offset : " << offset
                               << " bytes to read: " << iov.iov_len;
            }
          
            std::string store_id;
            map_store_id (loc->get_product (), loc->get_store (), store_id);

            static iopx_tree_cfg_t src_cfg = {
                                                 loc->get_product (),
                                                 store_id,
                                                 std::string ("source"),
                                                 true,
                                                 false,
                                                 0,
                                                 true,
                                                 fd_cache_size
                                             }; 

            alloc_src_iopx (src_cfg);

            if (!source) {

                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to allocate source iopx tree";
                std::error_code ec (ENOMEM, std::generic_category());
                return (ec); 
            }

            /*
             * We will use fast ioservice for scheduling source iopx fops.
             */ 

            src_iosvc = engine->get_ioservice (true);

            src_iosvc->post (boost::bind (&data_mgmt::read_splice, this,
                                          loc, offset, iov, cbki));

            return (openarchive::success);
        }
            
        std::error_code data_mgmt::read_splice (arch_loc_ptr_t loc,
                                                uint64_t offset,
                                                const struct iovec iov, 
                                                arch_store_cbk_info_ptr_t cbki)
        {

            if (log_level >= openarchive::logger::level_debug_2) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                               << " offset : " << offset
                               << " bytes to read: " << iov.iov_len;
            }

            /*
             * Allocate a file descriptor from memory pool local to the
             * thread.
             */  
            tls_ref_t tls_ref = openarchive::arch_tls::get_arch_tls ();
            file_ptr_t fp = tls_ref->alloc_arch_file ();
            fp->set_loc (loc);
            fp->set_iopx (source);

            req_ptr_t req = tls_ref->alloc_iopx_req ();
            openarchive::iopx_req::init_open_req (fp, req, 
                                                  O_RDONLY | O_NOATIME); 

            std::error_code ec = source->open (fp, req);
            if (ec != ok) {

                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " open failed for " << loc->get_uuidstr ()
                               << " error code: " << ec.value ()
                               << " error desc: " << strerror (ec.value ());
                return(ec);
            }

            init_read_req  (fp, req, offset, iov.iov_len, 0, iov.iov_base);
            req->set_asyncio (true);

            /*
             * Save callback info to be played back after completion of 
             * read request
             */ 
            file_info_t info;
            info.set_cbk_info (cbki);
            fp->set_file_info ("arch_store", info);

            ec =  source->pread (fp, req);

            if (ec != ok) {

                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV(log, openarchive::logger::level_error)
                              << " pread failed for " << loc->get_uuidstr () 
                              << " error code: " << ec.value ()
                              << " error desc: " << strerror (ec.value ());
                return(ec);

            }

            return (openarchive::success);

        }

        void data_mgmt::work_done_cbk (arch_store_cbk_info_ptr_t cbk, 
                                       dmstats_ptr_t dmp, int32_t ret, 
                                       int32_t errnum)
        {
            uint64_t pending = dmp->decr_pending (1);

            if (dmp->get_done() && !pending) {

                /*
                 * All the backup requests have been submitted and 
                 * have been processed. Time to invoke the callback
                 * handler to inform the app of completion.
                 */ 
                if (cbk->incr_req () == 0) {
                    /*
                     * Invoke callback handler
                     */   
                    cbk->set_ret_code (ret);
                    cbk->set_err_code (errnum);
                    cbk->set_done (true);

                    /*
                     * Now invoke the callback
                     */
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " Invoking callback handler";

                    openarchive::arch_store::arch_store_callback_t callback = cbk->get_store_cbk ();
                    callback (cbk);
                }   
            }
        } 

        std::error_code data_mgmt::sendfile (file_ptr_t src_fp, 
                                             file_ptr_t dest_fp, 
                                             uint64_t offset, buff_ptr_t bufp,
                                             uint64_t bytes, uint64_t & sent)
        {
            /*
             * Sendfile is the method which saves the file to archive store.
             */   
            tls_ref_t tls_ref = openarchive::arch_tls::get_arch_tls ();
            req_ptr_t req = tls_ref->alloc_iopx_req ();


            openarchive::iopx_req::init_read_req  (src_fp, req, offset, bytes, 
                                                   0, bufp);

            std::error_code ec = source->pread (src_fp, req);
            if (ec != ok) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " read failed for file " 
                               << src_fp->get_loc ().get_pathstr () 
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);
                return(ec);
            }

            bytes = req->get_ret ();

            if (bytes > 0) {

                /*
                 * The data read should be written to archive store.
                 */ 
                openarchive::iopx_req::init_write_req  (dest_fp, req, offset, bytes,
                                                        0, bufp);

                ec = sink->pwrite (dest_fp, req);
                if (ec != ok) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " write failed for file " 
                                   << dest_fp->get_loc ().get_pathstr ()
                                   << " error code: " << errno
                                   << " error desc: " << strerror(errno);
                    return(ec);
                }

            }

            sent = req->get_ret ();

            if (log_level >= openarchive::logger::level_debug_2) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                               << " sent data from " 
                               << src_fp->get_loc ().get_pathstr () << " to "
                               << dest_fp->get_loc ().get_pathstr ()
                               << " expected: " << bytes
                               << " actual: " << sent;
            }

            return (openarchive::success);
        }

        std::error_code data_mgmt::get_file_size (iopx_ptr_t iopx, 
                                                  file_ptr_t fp,
                                                  req_ptr_t req, 
                                                  uint64_t & size)
        {
            /*
             * Extract the size of the file before archival.
             */
            uint64_t file_size;
            struct iovec iov;
            iov.iov_base = &(file_size);
            iov.iov_len = sizeof (file_size);

            init_fgetxattr_req (fp, req, OPAR_XATTR_ARCHIVE_SIZE, &iov);

            std::error_code ec = source->fgetxattr (fp, req);

            if (ec == ok) {
                size = file_size;
            }

            return ec;
        }
 
        std::error_code data_mgmt::ftruncate (iopx_ptr_t iopx, file_ptr_t fp,
                                              req_ptr_t req, uint64_t size)
        {
            std::error_code ec;

            openarchive::iopx_req::init_ftruncate_req (fp, req, size);
            ec  = source->ftruncate (fp, req);

            return(ec);
        }

        std::error_code data_mgmt::fsetxattr (iopx_ptr_t source, file_ptr_t fp, 
                                              std::string name, void *val,
                                              size_t size)
        {
            std::error_code ec;

            tls_ref_t tls_ref = openarchive::arch_tls::get_arch_tls ();
            req_ptr_t req = tls_ref->alloc_iopx_req ();

            struct iovec work_iov;
            work_iov.iov_base = val;
            work_iov.iov_len = size;

            openarchive::iopx_req::init_fsetxattr_req (fp, req, name, 
                                                       &work_iov, 0);
            ec  = source->fsetxattr(fp, req);

            if (ec != ok && EEXIST == ec.value()) {

                req->set_flags (XATTR_REPLACE);
                ec  = source->fsetxattr(fp, req);

            }

            return(ec);

        }

        std::error_code data_mgmt::fsetxattrs (iopx_ptr_t source, file_ptr_t fp,
                                               file_attr_ptr_t fattr, 
                                               bool bkup, bool archv)
        {
            /*
             * We will set extended file system attributes so that stat done
             * on the archived file will pick the required attributes from 
             * the extended fs attributes.
             */
        
            std::error_code ec;

            if (archv) {

                uint64_t val = fattr->get_fsize ();
                ec = fsetxattr (source, fp, OPAR_XATTR_ARCHIVE_SIZE, 
                                &val, sizeof (val));

                if (ec != ok) {

                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV(log, openarchive::logger::level_error)
                                  << " failed to set " << OPAR_XATTR_ARCHIVE_SIZE 
                                  << " for " << fp->get_loc ().get_pathstr () 
                                  << " : " << fp->get_loc ().get_uuidstr ()
                                  << " error code: " << ec.value() 
                                  << " error desc: " << ec.message();
                    return(ec);

                }
               
                val = fattr->get_blocks ();  
                ec = fsetxattr (source, fp, OPAR_XATTR_ARCHIVE_BLOCKS, 
                                &val, sizeof (val));

                if (ec != ok) {

                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV(log, openarchive::logger::level_error)
                                  << " failed to set " << OPAR_XATTR_ARCHIVE_BLOCKS
                                  << " for " << fp->get_loc ().get_pathstr () 
                                  << " : " << fp->get_loc ().get_uuidstr ()
                                  << " error code: " << ec.value() 
                                  << " error desc: " << ec.message();
                    return(ec);

                }
               
                val = fattr->get_blksize ();
                ec = fsetxattr (source, fp, OPAR_XATTR_ARCHIVE_BLOCKSIZE, 
                                &val, sizeof (val));

                if (ec != ok) {

                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV(log, openarchive::logger::level_error)
                                  << " failed to set " 
                                  << OPAR_XATTR_ARCHIVE_BLOCKSIZE
                                  << " for " << fp->get_loc ().get_pathstr () 
                                  << " : " << fp->get_loc ().get_uuidstr ()
                                  << " error code: " << ec.value() 
                                  << " error desc: " << ec.message();
                    return(ec);

                }
            }

            if (bkup) {

                uuid_t uid;
                fattr->get_uuid (uid);

                ec = fsetxattr (source, fp, OPAR_XATTR_ARCHIVE_UUID, 
                                &(uid), sizeof(uid));

                if (ec != ok) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV(log, openarchive::logger::level_error)
                                  << " failed to set " 
                                  << OPAR_XATTR_ARCHIVE_BLOCKSIZE
                                  << " for " << fp->get_loc ().get_pathstr () 
                                  << " : " << fp->get_loc ().get_uuidstr ()
                                  << " error code: " << ec.value() 
                                  << " error desc: " << ec.message();
                    return(ec);
                }

                std::string attr = fattr->get_product ();
                ec = fsetxattr (source, fp, OPAR_XATTR_PRODUCT_ID, 
                                (void *) attr.c_str(), attr.length ());

                if (ec != ok) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV(log, openarchive::logger::level_error)
                                  << " failed to set " 
                                  << OPAR_XATTR_PRODUCT_ID 
                                  << " for " << fp->get_loc ().get_pathstr () 
                                  << " : " << fp->get_loc ().get_uuidstr ()
                                  << " error code: " << ec.value() 
                                  << " error desc: " << ec.message();
                    return(ec);
                }

                attr = fattr->get_store (); 
                ec = fsetxattr (source, fp, OPAR_XATTR_STORE_ID, 
                                (void *) attr.c_str(), attr.length ());

                if (ec != ok) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV(log, openarchive::logger::level_error)
                                  << " failed to set " 
                                  << OPAR_XATTR_STORE_ID 
                                  << " for " << fp->get_loc ().get_pathstr () 
                                  << " : " << fp->get_loc ().get_uuidstr ()
                                  << " error code: " << ec.value() 
                                  << " error desc: " << ec.message();
                }
            }

            return(ec);

        }

        void data_mgmt::alloc_src_iopx (iopx_tree_cfg_t & cfg)
        {
            openarchive::arch_core::spinlock_handle handle(lock);

            if (!source) {
                source = engine->mktree (cfg);
            } 

            return;
        }

        void data_mgmt::alloc_sink_iopx (iopx_tree_cfg_t & cfg)
        {
            openarchive::arch_core::spinlock_handle handle(lock);

            if (!sink) {
                sink = engine->mktree (cfg);
            } 

            return;
        }

        void data_mgmt::map_store_id (std::string &product,
                                      std::string &inp_store, 
                                      std::string &outp_store)
        {
            engine->map_store_id (product, inp_store, outp_store);
            return; 
        }

        void data_mgmt::mem_prof (void)
        {
            uint32_t count = 0;
         
            while(!done.load()) {

                /*
                 * Log the memory consumtion once every 30 minutes
                 */
                if (!(count % 18000)) {
                    log_memory_stats ();
                    count = 0;
                }
 
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                count++;  

            }  

            if (count != 1) {
                log_memory_stats ();
            }

            return;
        }
 
        void data_mgmt::log_memory_stats (void)
        {
            /*
             * Log the object pool statistics
             */
            std::string memstats;

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << "Object pools statistics:";
            }

            cbk_pool.getstats (memstats);
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << memstats;
            }

            file_pool.getstats (memstats);
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << memstats;
            }

            req_pool.getstats (memstats);
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << memstats;
            }

            dmstat_pool.getstats (memstats);
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << memstats;
            }

            fileattr_pool.getstats (memstats);
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << memstats;
            }

            loc_pool.getstats (memstats); 
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << memstats;
            }

            /*
             * Log the statistics about memory that was allocated through 
             * malloc/jemalloc
             */
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << "malloc interface statistics:";
            }

            malloc_intfx_ptr_t mptr = openarchive::arch_mem::get_malloc_intfx();
            mptr->getstats (memstats);

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << memstats;
            }

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << "thread local storage statistics:";
            }
            log_tls_stats ();

            /*
             * Invoke the profiling for source and sink iopx.
             */ 
            if (source) {
                source->profile ();
            }

            if (sink) {
                sink->profile ();
            }

            return;

        }

        void data_mgmt::log_tls_stats (void)
        {
            bool bfast = openarchive::cfgparams::create_fast_threads ();
            bool bslow = openarchive::cfgparams::create_slow_threads ();

            if (bfast) {
                /* 
                 * fast iosvc is enabled. Log statistics.
                 */ 
                io_service_ptr_t fast_iosvc = engine->get_ioservice (true);
                if (fast_iosvc) {
                    /*
                     * We are making a guess here and we submit requests to
                     * the thread pool, assuming that each new request is 
                     * handed over to a different thread. In cases where
                     * multiple requests are handed to the same thread we 
                     * might end up logging the same information twice. 
                     */
                    uint32_t nthreads = boost::thread::hardware_concurrency();
                    for(uint32_t count = 0; count < nthreads; count++) {
                        fast_iosvc->post(boost::bind(&data_mgmt::log_tls_info, 
                                                     this));
                    }
                }
            }

            if (bslow) {
                /* 
                 * slow iosvc is enabled. Log statistics.
                 */ 
                io_service_ptr_t slow_iosvc = engine->get_ioservice (false);
                if (slow_iosvc) {
                    uint32_t nthreads = boost::thread::hardware_concurrency();
                    for(uint32_t count = 0; count < nthreads; count++) {
                        slow_iosvc->post(boost::bind(&data_mgmt::log_tls_info, 
                                                     this));
                    }
                }
            }

            return;
        } 

        void data_mgmt::log_tls_info (void)
        {
            tls_ref_t tls_ref = openarchive::arch_tls::get_arch_tls ();

            tls_ref->log_statistics ();

            return;
        }

    } /* End of namespace data_mgmt */

} /* End of namespace openarchive */
