/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <perf_iopx.h>

namespace openarchive
{
    namespace perf_iopx
    {
        perf_iopx::perf_iopx (std::string name, io_service_ptr_t svc):
                              openarchive::arch_iopx::arch_iopx (name, svc)
        {
            seq.store (1);
            open_count.store (0);
            close_count.store (0);
            pread_count.store (0);
            pwrite_count.store (0);
            fstat_count.store (0);
            stat_count.store (0);
            ftruncate_count.store (0);
            truncate_count.store (0);
            fsetxattr_count.store (0);
            setxattr_count.store (0);
            fgetxattr_count.store (0);
            getxattr_count.store (0);
            fremovexattr_count.store (0);
            removexattr_count.store (0);
            lseek_count.store (0);
            getuuid_count.store (0);
            gethosts_count.store (0);
            mkdir_count.store (0);
            resolve_count.store (0);
            dup_count.store (0);

            open_time.store (0);
            close_time.store (0);
            pread_time.store (0);
            pwrite_time.store (0);
            fstat_time.store (0);
            stat_time.store (0);
            ftruncate_time.store (0);
            truncate_time.store (0);
            fsetxattr_time.store (0);
            setxattr_time.store (0);
            fgetxattr_time.store (0);
            getxattr_time.store (0);
            fremovexattr_time.store (0);
            removexattr_time.store (0);
            lseek_time.store (0);
            getuuid_time.store (0);
            gethosts_time.store (0);
            mkdir_time.store (0);
            resolve_time.store (0);
            dup_time.store (0);

            bytes_read.store (0);
            bytes_written.store (0); 
        
            log_level = openarchive::cfgparams::get_log_level();

            return; 
        }

        perf_iopx::~perf_iopx (void)
        {
        }

        std::error_code perf_iopx::open (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->open (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
            open_time.fetch_add (delta);
            open_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::close (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->close (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            close_time.fetch_add (delta);
            close_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::close (file_t & fp)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->close (fp);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            close_time.fetch_add (delta);
            close_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::pread (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;

            if (req->get_asyncio ()) {
                return pread_async (fp, req);
            }
 
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->pread (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            pread_time.fetch_add (delta);
            pread_count.fetch_add (1);

            int64_t bytes = req->get_ret ();
            if (bytes > 0) {
                bytes_read.fetch_add (bytes);
            }

            return ec;
        }

        std::error_code perf_iopx::pread_async (file_ptr_t fp, req_ptr_t req)
        {
            uint64_t num = seq.fetch_add (1);
                   
            req_info info;
            info.start = std::chrono::high_resolution_clock::now();

            if (!request_map.insert (num, info)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to insert entry in request map for"
                               << " file " << fp->get_loc().get_pathstr();

                return std::error_code (EBADSLT, std::generic_category());
            }

            /*
             * Add an entry for this iopx in the req object
             */
            if (false == req->set_id (get_name (), num)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to insert entry in id map for file "
                               << fp->get_loc().get_pathstr();

                return std::error_code (EBADSLT, std::generic_category());
            }

            std::error_code ec = get_first_child ()->pread (fp, req);
            return ec;
        }

        std::error_code perf_iopx::pread_cbk (file_ptr_t fp, req_ptr_t req,
                                              std::error_code ec)
        {
            /*
             * Get the ID for this request from the idmap inside request.
             */
            uint64_t id;

            if (req->get_id (get_name (), id)) {

                /*
                 * Got id generated by this iopx from the request object
                 */
                req_info info;
                
                if (request_map.extract (id, info)) {

                    if (log_level >= openarchive::logger::level_debug_2) {

                        BOOST_LOG_FUNCTION ();
                        BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                       << " found an entry with id " << id
                                       << " for " << fp->get_loc().get_uuidstr()
                                       << " in request map ret : "
                                       << req->get_ret ();
                    }

                    /*
                     * We got the info object that was generated as part of 
                     * processing the read request.
                     */ 
                    std::chrono::high_resolution_clock::time_point end;
                    end = std::chrono::high_resolution_clock::now();

                    uint64_t delta = std::chrono::duration_cast<std::chrono::microseconds>(end - info.start).count();

                    pread_time.fetch_add (delta);
                    pread_count.fetch_add (1);

                    int64_t bytes = req->get_ret ();
                    if (bytes > 0) {
                        bytes_read.fetch_add (bytes);
                    }

                    /*
                     * Invoke the completion handler for the request.
                     */
                    work_done_cbk (fp, req, ec); 

                    request_map.erase (id);
                } 

                req->erase_id (get_name ());

            } else {

                if (log_level >= openarchive::logger::level_error) {

                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " failed to find id for "
                                   << fp->get_loc().get_uuidstr();
                }
            }
             
            return openarchive::success;
        }

        std::error_code perf_iopx::pwrite (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->pwrite (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            pwrite_time.fetch_add (delta);
            pwrite_count.fetch_add (1);

            int64_t bytes = req->get_ret ();
            if (bytes > 0) {
                bytes_written.fetch_add (bytes);
            }

            return ec;
        }

        std::error_code perf_iopx::fstat (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->fstat (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            fstat_time.fetch_add (delta);
            fstat_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::stat (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->stat (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            stat_time.fetch_add (delta);
            stat_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::ftruncate (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->ftruncate (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            ftruncate_time.fetch_add (delta);
            ftruncate_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::truncate (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->truncate (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            truncate_time.fetch_add (delta);
            truncate_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::fsetxattr (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->fsetxattr (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            fsetxattr_time.fetch_add (delta);
            fsetxattr_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::setxattr (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->setxattr (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            setxattr_time.fetch_add (delta);
            setxattr_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::fgetxattr (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->fgetxattr (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            fgetxattr_time.fetch_add (delta);
            fgetxattr_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::getxattr (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->getxattr (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            getxattr_time.fetch_add (delta);
            getxattr_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::fremovexattr (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->fremovexattr (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            fremovexattr_time.fetch_add (delta);
            fremovexattr_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::removexattr (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->removexattr (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            removexattr_time.fetch_add (delta);
            removexattr_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::lseek (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->lseek (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            lseek_time.fetch_add (delta);
            lseek_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::getuuid (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->getuuid (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            getuuid_time.fetch_add (delta);
            getuuid_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::gethosts (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->gethosts (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            gethosts_time.fetch_add (delta);
            gethosts_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::mkdir (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->mkdir (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            mkdir_time.fetch_add (delta);
            mkdir_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::resolve (file_ptr_t fp, req_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->resolve (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            resolve_time.fetch_add (delta);
            resolve_count.fetch_add (1);

            return ec;
        }

        std::error_code perf_iopx::dup (file_ptr_t fp, file_ptr_t req)
        {
            std::chrono::high_resolution_clock::time_point start, end;
            uint64_t delta;
        
            start = std::chrono::high_resolution_clock::now();
            std::error_code ec = get_first_child ()->dup (fp, req);
            end = std::chrono::high_resolution_clock::now();

            delta = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            dup_time.fetch_add (delta);
            dup_count.fetch_add (1);

            return ec;
        }

        void perf_iopx::profile (void)
        {
            log_avg_time ("open", open_time, open_count);
            log_avg_time ("close", close_time, close_count);
            log_avg_time ("pread", pread_time, pread_count);
            log_avg_time ("pwrite", pwrite_time, pwrite_count);
            log_avg_time ("fstat", fstat_time, fstat_count);
            log_avg_time ("stat", stat_time, stat_count);
            log_avg_time ("ftruncate", ftruncate_time, ftruncate_count);
            log_avg_time ("truncate", truncate_time, truncate_count);
            log_avg_time ("fsetxattr", fsetxattr_time, fsetxattr_count);
            log_avg_time ("setxattr", setxattr_time, setxattr_count);
            log_avg_time ("fgetxattr", fgetxattr_time, fgetxattr_count);
            log_avg_time ("getxattr", getxattr_time, getxattr_count);
            log_avg_time ("fremovexattr", fremovexattr_time, fremovexattr_count);
            log_avg_time ("fremovexattr", removexattr_time, removexattr_count);
            log_avg_time ("lseek", lseek_time, lseek_count);
            log_avg_time ("getuuid", getuuid_time, getuuid_count);
            log_avg_time ("gethosts", gethosts_time, gethosts_count);
            log_avg_time ("mkdir", mkdir_time, mkdir_count);
            log_avg_time ("resolve", resolve_time, resolve_count);
            log_avg_time ("dup", dup_time, dup_count); 

            log_throughput ("pread", pread_time, bytes_read);
            log_throughput ("pwrite", pwrite_time, bytes_written);
 
            get_first_child ()->profile ();
           
            return;
        }

        void perf_iopx::log_avg_time (std::string fop, 
                                      std::atomic <uint64_t> &time, 
                                      std::atomic <uint64_t> &samples)
        {
            uint64_t uitotal_time = time.load ();
            double dtotal_time = (double) uitotal_time;
            uint64_t count = samples.load ();
            double avg_time = 0.0;

            if (!count) {
                return;
            }

            avg_time = dtotal_time/count;

            if (log_level >= openarchive::logger::level_error) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << fop << " samples: " << count
                               << " elapsed time: " << uitotal_time << " microsec"
                               << " average time: " 
                               << boost::format ("%.3f") % avg_time 
                               << " microsec";
            }

            return;
        } 

        void perf_iopx::log_throughput (std::string fop,
                                        std::atomic <uint64_t> &time,
                                        std::atomic <uint64_t> &bytes)
        {
            uint64_t uitotal_bytes = bytes.load ();
            double dtotal_bytes = (double) uitotal_bytes;
            uint64_t usec = time.load ();
            double thput = 0.0;
            static const double thfac = 0.95367;

            if (!usec) {
                return;
            }
 
            /*
             * To convert bytes B and time T in usec to MB/sec
             * We will use the formula (B*1000000)/(time*1024*1024) 
             * 1000000/(1024*1024) = 0.95367
             * So (B*0.95367)/time will be throughput in MB/sec
             */  

            thput = ((dtotal_bytes*thfac)/usec);

            if (log_level >= openarchive::logger::level_error) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << fop << " total bytes : " << uitotal_bytes
                               << " elapsed time: " << usec << " microsec"
                               << " throughput : " 
                               << boost::format ("%.3f") % thput 
                               << " MB/Sec";
            }

            return;
        } 
                              
        void perf_iopx::work_done_cbk (file_ptr_t fp, req_ptr_t req, 
                                       std::error_code ec)
        {
            /*
             * Invoke callback handler to inform the app about completion status
             */
            file_info_t info;

            if (!fp->get_file_info ("arch_store", info)) {
                assert (0);
            }

            arch_store_cbk_info_ptr_t cbk = info.get_cbk_info ();

            if (cbk->incr_req () == 0) {
                /*
                 * Invoke callback handler
                 */   
                cbk->set_ret_code (req->get_ret ());
                cbk->set_err_code (ec.value ());
                cbk->set_done (true);

                /*
                 * Remove the slots
                 */
                fp->erase_file_info ("arch_store");

                /*
                 * Now invoke the callback
                 */
                openarchive::arch_store::arch_store_callback_t callback;
                callback = cbk->get_store_cbk ();
                callback (cbk);
        
                if (log_level >= openarchive::logger::level_debug_2) {

                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " app callback handler invoked for "
                                   << fp->get_loc ().get_uuidstr ()
                                   << " ret : " << cbk->get_ret_code ();
                }

            }   

            return;
        }

    }

} 
