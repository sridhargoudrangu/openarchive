#include <cvlt_iopx.h>
#include <arch_tls.h>

namespace openarchive
{
    namespace cvlt_iopx
    {
        const std::string cvoblib = "/opt/commvault/Base/libCVOpenBackupStatic.so";

        void log_error (CVOB_hError *perror, 
                        src::severity_logger<int> &log, 
                        libcvob_fops_t & fptrs, std::string func)
        {
            int32_t err_code;
            char err_str[1024];

            fptrs.get_error (perror, &err_code, err_str, sizeof(err_str)); 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << func << " failed"
                               << " error code: " << err_code
                               << " error desc: " << err_str;
            }

            if (perror) {
                fptrs.free_error (perror);
            }

            return;
        }

        cvlt_iopx::cvlt_iopx (std::string name, io_service_ptr_t svc,
                              std::string info, uint32_t num_threads):
                              openarchive::arch_iopx::arch_iopx (name, svc),
                              fops (cvoblib), fptrs(fops.get_fops()), 
                              args (info), ready(false), comcell_id (-1),
                              proxy_name (""), proxy_port (-1), app_type (-1), 
                              client_name (""), instance_name (""),
                              backupset_name (""), subclient_name (""), 
                              cvobsession (NULL), job_id (0), job_token (""),
                              job_type (CVLT_UNKNOWN_JOB), cvobjob (NULL),
                              num_streams (0), cvstreammgr (NULL), seq (1),
                              ctx_pool ("cvctx-pool"), 
                              num_worker_threads (num_threads)
        {
            log_level = openarchive::cfgparams::get_log_level();

            ready = fops.is_valid ();
            if (!ready) {
                return;
            }

            set_cvlt_logging (); 

            if (parse_args () != ok) {
                ready = false;
                return;
            }

            if (!validate_params ()) {
                ready = false;
                return;
            }

            /*
             * We need to create a openbackup session with direct
             * pipeline configuration. openbackup provides 2 kinds of 
             * initializations.
             * 1) Init
             *    This supports the functionality to create direct pipeline.
             *    But expects numerical arguments, which is not a very 
             *    portable interface.
             * 2) Init2
             *    This works with names but does not support the 
             *    functionality to create a direct pipeline.
             *
             * To handle this we will first create a Init2 sessions and map
             * the names to numerical ID's. After that we will release this 
             * session and will create a session through Init interface to 
             * work with direct pipe-line. This is not very efficient but
             * until an mixed interface comes up we will continue to operate
             * this way.
             */


            if (extract_ids () != ok) {
                ready = false;
                return;
            } 

            /*
             * Now that the id's have been extracted we will try to 
             * establish a OB session of direct pipe line type.
             */  
            if (allocsession() != ok) {
                ready = false;
                return;
            } 

            /*
             * Start the data management job.
             */
            if (startjob () != ok) {
                ready = false;
                return;
            }

            if (job_type == CVLT_FULL_BACKUP || job_type == CVLT_INCR_BACKUP ||
                job_type == CVLT_RESTORE) {

                /*
                 * Start the streams 
                 */ 
                cvstreammgr = new cvlt_stream_manager (cvobjob, num_streams, 
                                                       fptrs, 
                                                       num_worker_threads);
                if (!cvstreammgr) {
                    ready = false;
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " failed to allocate stream manager";
                    return;
                }
            }

            if (job_type == CVLT_RESTORE) {
                /*
                 * In case of restores enable stream reservation by default.
                 */  
                cvstreammgr->enable_stream_reservation ();
            }

            if (log_level >= openarchive::logger::level_error) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " Successfully initialized cvlt iopx";
            }

            return; 
        }

        cvlt_iopx::~cvlt_iopx (void)
        {
            if (ready) {
                /*
                 * Release all the streams that have been allocated/reserved 
                 * for this job.
                 */
                if (cvstreammgr) {
                    delete cvstreammgr;
                }

                stopjob (true);
                releasesession ();
            }

            return; 
        }

        std::error_code cvlt_iopx::open (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            if (job_type == CVLT_FULL_BACKUP || job_type == CVLT_INCR_BACKUP) {

                /*
                 * If the data management operation type is backup and file
                 * is being opened then we will allocate a stream to be used for
                 * backup.
                 */

                std::string path = fp->get_loc ().get_pathstr ();
                std::string uuid = fp->get_loc ().get_uuidstr ();

                cvlt_stream * stream = alloc_stream (path);
                if (!stream) {
                    /*
                     * Failed to allocate a stream
                     */
                    return (std::error_code (ENOSR, std::generic_category()));
                }

                /*
                 * Now that the stream has been allocated, allocate the item
                 * that will be used for writing the contents.
                 */

                std::error_code ec = stream->alloc_item (path, uuid, 
                                                         req->get_len ());
                if (ec != ok) {
                    return ec;
                }

                std::string genuuid;
                cvuuid_to_v4uuid (stream->get_guid (), genuuid);
                req->set_info (genuuid);

                /*
                 * Write the metadata corresponding to this file
                 */
                char mdbuf[64]; 
                cvmd md;
                fp->get_loc ().get_uuid (md.uuid);
                md.file_len = req->get_len ();  

                size_t sz = cvmdserialize (md, mdbuf, sizeof (mdbuf));
                if (sz < 0) {
                    return (std::error_code (ENOSPC, std::generic_category()));
                }

                ec = stream->send_metadata (0x01, mdbuf, sizeof (mdbuf));
                if (ec != ok) {
                    return ec;
                }

                /*
                 * Save the stream for future fops.
                 */ 
                file_info_t info;
                info.set_cvlt_stream (stream);
                fp->set_file_info (get_name(), info);

            } else if (job_type == CVLT_RESTORE) {

                /*
                 * Initialize the file size
                 */
                fp->set_file_size (LLONG_MAX); 

            }

            /*
             * The file has been opened successfully. Increase the ref count. 
             */ 
            get();

            return openarchive::success;
        }

        std::error_code cvlt_iopx::close (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            /*
             * Decrease the ref count. 
             */ 
            put();

            if (job_type == CVLT_FULL_BACKUP || job_type == CVLT_INCR_BACKUP) {

                file_info_t info;
                if (!fp->get_file_info (get_name(), info)) {

                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                                   << " failed to find cvlt stream for file "
                                   << fp->get_loc().get_pathstr();

                    return (std::error_code (ENOENT, std::generic_category()));
                }

                std::error_code ec = release_resources (info);
                if (ec != ok) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " failed to release resources for file "
                                   << fp->get_loc().get_pathstr();
                    return ec;
                }
           
                /*
                 * Drop the stream from map.
                 */
                fp->erase_file_info (get_name());
            }
                
            return openarchive::success;

        }

        std::error_code cvlt_iopx::close (file_t &fp)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            /*
             * Decrease the ref count. 
             */ 
            put();

            if (job_type == CVLT_FULL_BACKUP || job_type == CVLT_INCR_BACKUP) {

                file_info_t info;
                if (!fp.get_file_info (get_name(), info)) {

                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                                   << " failed to find cvlt stream for file "
                                   << fp.get_loc().get_pathstr();

                    return (std::error_code (ENOENT, std::generic_category()));
                }

                std::error_code ec = release_resources (info);
                if (ec != ok) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " failed to release resources for file "
                                   << fp.get_loc().get_pathstr();
                    return ec;
                }

                /*
                 * Drop the stream from map.
                 */
                fp.erase_file_info (get_name());
            }

            return openarchive::success;
        }

        std::error_code cvlt_iopx::pread (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            if (job_type != CVLT_RESTORE) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " invalid FOP/job_type combination for file "
                               << fp->get_loc().get_pathstr();
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            size_t len = req->get_len ();
            uint64_t offset = req->get_offset ();   
            bool async_io = req->get_asyncio ();

            /*
             * Check whether offset exceeds size of the file.
             */
            if (offset >= fp->get_file_size ()) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " EOF condition for combination of offset: "
                               << offset <<" size: " << len << " for file "
                               << fp->get_loc().get_pathstr();

                req->set_ret (0);

                if (async_io) { 
                    /*
                     * Inform the parent pread callback handler of EOF condition
                     */   
                    pread_cbk (req->get_fptr (), req, openarchive::success);
                }
 
                return openarchive::success;
            }

            uuid_t uid;
            fp->get_loc().get_uuid (uid);
            std::string cvguid;
            v4uuid_to_cvuuid (uid, cvguid);

            void *buff = NULL;
            buff = openarchive::iopx_req::get_buff_baseaddr (req);
            assert (buff != NULL);

            uint64_t num = seq.fetch_add (1);
            int64_t ret; 

            if (!request_map.insert (num, req)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to insert entry in map for file "
                               << fp->get_loc().get_pathstr() << "  "
                               << cvguid;

                return std::error_code (EBADSLT, std::generic_category());
            }

            std::error_code ec = receive_data (num, cvguid, (char *) buff, len,
                                               offset, async_io, ret);

            if (ec != ok) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " receive_data failed for file "
                               << fp->get_loc().get_pathstr() << "  "
                               << cvguid << " error code : " << ec.value()
                               << " error desc : " << strerror (ec.value ());

                /*
                 * Remove the entry from request map.
                 */
                request_map.erase (num);

                return ec;
            }
           
            if (!async_io) { 

                req->set_ret (ret);

            }

            return openarchive::success;
   
        } 

        std::error_code cvlt_iopx::pwrite (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            if (job_type == CVLT_FULL_BACKUP || job_type == CVLT_INCR_BACKUP) {

                file_info_t info;
                if (!fp->get_file_info (get_name(), info)) {

                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                                   << " failed to find cvlt stream for file "
                                   << fp->get_loc().get_pathstr();

                    return (std::error_code (ENOENT, std::generic_category()));
                }

                cvlt_stream * stream = info.get_cvlt_stream (); 
                if (!stream) {
                    return (std::error_code (ENOSR, std::generic_category()));
                }

                void *buff = NULL;
                buff = openarchive::iopx_req::get_buff_baseaddr (req);
                assert (buff != NULL);

                size_t len = req->get_len ();

                return stream->send_data ((char *)buff, len);
            } 

            return (std::error_code (ENOSYS, std::generic_category()));
        } 

        std::error_code cvlt_iopx::fstat (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            return (std::error_code (ENOSYS, std::generic_category()));
        }

        std::error_code cvlt_iopx::stat (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            return (std::error_code (ENOSYS, std::generic_category()));
        }

        std::error_code cvlt_iopx::ftruncate (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            return (std::error_code (ENOSYS, std::generic_category()));
        }

        std::error_code cvlt_iopx::truncate (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            return (std::error_code (ENOSYS, std::generic_category()));
        }

        std::error_code cvlt_iopx::fsetxattr (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            return (std::error_code (ENOSYS, std::generic_category()));
        }

        std::error_code cvlt_iopx::setxattr (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            return (std::error_code (ENOSYS, std::generic_category()));
        } 

        std::error_code cvlt_iopx::fgetxattr (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            return (std::error_code (ENOSYS, std::generic_category()));
        }

        std::error_code cvlt_iopx::getxattr (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            return (std::error_code (ENOSYS, std::generic_category()));
        }  

        std::error_code cvlt_iopx::fremovexattr (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            return (std::error_code (ENOSYS, std::generic_category()));
        }

        std::error_code cvlt_iopx::removexattr (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            return (std::error_code (ENOSYS, std::generic_category()));
        }

        std::error_code cvlt_iopx::lseek (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            return (std::error_code (ENOSYS, std::generic_category()));
        }

        std::error_code cvlt_iopx::getuuid (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            return (std::error_code (ENOSYS, std::generic_category()));
        }

        std::error_code cvlt_iopx::gethosts (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            return (std::error_code (ENOSYS, std::generic_category()));
        }

        std::error_code cvlt_iopx::mkdir (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            return (std::error_code (ENOSYS, std::generic_category()));
        }

        std::error_code cvlt_iopx::resolve (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            return (std::error_code (ENOSYS, std::generic_category()));
        }

        std::error_code cvlt_iopx::dup (file_ptr_t src_fp, file_ptr_t dest_fp)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            if (job_type == CVLT_RESTORE) {

                /*
                 * Initialize the file size
                 */
                dest_fp->set_file_size (LLONG_MAX); 

                /*
                 * Increase the ref count. 
                 */ 
                get();

                return openarchive::success;

            } else {

                return (std::error_code (ENOSYS, std::generic_category()));
            }
        } 

        std::error_code cvlt_iopx::scan (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cvlt iopx not ready";
                }
                return (std::error_code (ENOTCONN, std::generic_category()));
            }  

            return (std::error_code (ENOSYS, std::generic_category()));
        }

        std::error_code cvlt_iopx::release_resources (file_info_t &info)
        {
            cvlt_stream * stream = info.get_cvlt_stream (); 
            if (!stream) {
                return (std::error_code (ENOSR, std::generic_category()));
            }
      
            stream->set_active (false); 
            stream->release_item ();
            cvstreammgr->release_stream (stream);

            return openarchive::success;
        }
 
        std::error_code cvlt_iopx::parse_args (void)
        {
            /*
             * Parse the supplied arguments string and extract the parameters
             * The argument string is expected to be in the following format.
             * "cc=2:cn=node1:ph=node2:pp=8400:at=29:in=Instance001:bs=idm:"
             * "sc=arch:ji=124:jt=full-backup:ns=16"
             * The meaning of each argument label is explained below:
             * cc - CommCell ID
             * cn - Commvault Host Name
             * ph - Proxy host name
             * pp - Proxy port number
             * at - App type. The valid values for app type are
             *      29 - Linux File system APP type
             * in - Instance Name 
             * bs - BackupSet Name
             * sc - SubClient Name
             * ji - Job ID
             * jk - Job token
             * jt - Job Type. Valid values are 
             *      browse, full-backup, incr-backup, restore
             * ns - Number of streams
             */ 
            openarchive::arch_core::arg_parser parser(args);

            /*
             * All the arguments have been extracted. Parse the arguments 
             * and extract parameters.
             */
            std::string value;
            std::error_code ec = parser.extract_param ("cc", value);
            if (ok != ec) {
                return ec;
            }
            comcell_id = boost::lexical_cast<int32_t> (value); 

            ec = parser.extract_param ("cn", value);
            if (ok != ec) {
                return ec;
            }
            client_name = value;
 
            ec = parser.extract_param ("ph", value);
            if (ok != ec) {
                return ec;
            }
            proxy_name = value;

            ec = parser.extract_param ("pp", value);
            if (ok != ec) {
                return ec;
            }
            proxy_port = boost::lexical_cast<int32_t> (value); 

            ec = parser.extract_param ("at", value);
            if (ok != ec) {
                return ec;
            }
            app_type = boost::lexical_cast<int32_t> (value); 

            ec = parser.extract_param ("in", value);
            if (ok != ec) {
                return ec;
            }
            instance_name = value;

            ec = parser.extract_param ("bs", value);
            if (ok != ec) {
                return ec;
            }
            backupset_name = value;

            ec = parser.extract_param ("sc", value);
            if (ok != ec) {
                return ec;
            }
            subclient_name = value;

            ec = parser.extract_param ("ji", value);
            if (ok == ec) {
                job_id = boost::lexical_cast<uint32_t> (value); 
            }

            ec = parser.extract_param ("jk", value);
            if (ok == ec) {
                job_token = value;
            }

            ec = parser.extract_param ("jt", value);
            if (ok != ec) {
                return ec;
            }

            if (value == "browse") {
                job_type = CVLT_BROWSE;
            } else if (value == "full-backup") {
                job_type = CVLT_FULL_BACKUP;
            } else if (value == "incr-backup") {
                job_type = CVLT_INCR_BACKUP;
            } else if (value == "restore") {
                job_type = CVLT_RESTORE;
            }

            ec = parser.extract_param ("ns", value);
            if (ok != ec) {
                return ec;
            }
            num_streams = boost::lexical_cast<int32_t> (value); 

            return openarchive::success;
        }

        bool cvlt_iopx::validate_params (void)
        {
            if ((comcell_id > 0)               &&
                (proxy_name.length ())         &&
                (proxy_port > 0)               &&
                (app_type > 0)                 &&
                (client_name.length ())        &&
                (instance_name.length ())      &&
                (backupset_name.length ())     &&
                (subclient_name.length ())     &&
                (job_type != CVLT_UNKNOWN_JOB) &&
                (num_streams > 0))             {
                return true;
            } 

            return false;
        }


        std::error_code cvlt_iopx::extract_ids (void)
        {
            if (!ready) {
                return (std::error_code (EPERM, std::generic_category()));
            }
 
            CertificateInfo_t cert;

            cert.clientName = "";
            cert.cretificateLocation = "";
            cert.privateKeyLocation = "";
            cert.publicKeyLocation = "";

            CVOB_hError * perror = NULL;

            ClientInfo_t clientInfo;
            memset (&clientInfo, 0, sizeof(ClientInfo_t));
            std::string security_token;

            int rc = fptrs.init2 (&cert, proxy_name.c_str (), 
                                 static_cast<int16_t>(proxy_port),
                                 client_name.c_str(), app_type,
                                 NULL, &clientInfo, &cvobsession,
                                 NULL, instance_name.c_str(), 
                                 backupset_name.c_str(),
                                 subclient_name.c_str(), security_token.c_str(),
                                 120, 0, NULL, &perror);

            if (rc) {
                log_error (perror, log, fptrs, 
                           std::string ("cvlt_iopx::extract_ids"));
                return (std::error_code (EFAULT, std::generic_category()));
            }

            /*
             * Session has been allocated successfully. Extract the ID's.
             */
            comcell_id = clientInfo.a_commCellId;
            client_id = clientInfo.a_clientId;
            app_id = clientInfo.a_appId;    

            /*
             * Required fields have been extracted. Release the session.
             */ 
            return (releasesession ());
        }

        std::error_code cvlt_iopx::allocsession (void)
        {
            if (!ready) {
                return (std::error_code (EPERM, std::generic_category()));
            }

            CertificateInfo_t cert;

            cert.clientName = "";
            cert.cretificateLocation = "";
            cert.privateKeyLocation = "";
            cert.publicKeyLocation = "";

            std::string configuration = "direct-pipeline:yes";
            CVOB_hError * perror = NULL;
            int32_t proxy_id = 0;

            int32_t rc = fptrs.init (&cert, proxy_name.c_str(), 
                                     static_cast<int16_t> (proxy_port),
                                     proxy_id, comcell_id, client_id,
                                     app_id, app_type, configuration.c_str (),
                                     NULL, &cvobsession, &perror);

            if (rc) {
                log_error (perror, log, fptrs, 
                           std::string ("cvlt_iopx::allocsession"));
                return (std::error_code (EFAULT, std::generic_category()));
            }

            return openarchive::success;
        }  

        std::error_code cvlt_iopx::releasesession (void)
        {
            if (!ready) {
                return (std::error_code (EPERM, std::generic_category()));
            }

            CVOB_hError * perror = NULL;

            int32_t rc = fptrs.deinit (cvobsession, 0, &perror);
            if (rc) {
                log_error (perror, log, fptrs, 
                           std::string ("cvlt_iopx::releasesession"));
                return (std::error_code (EFAULT, std::generic_category()));
            }

            return openarchive::success;
        }

        std::error_code cvlt_iopx::startjob (void)
        {
            if (!ready) {
                return (std::error_code (EPERM, std::generic_category()));
            }

            CVOB_JobType jt;
            int32_t options = 0;
            CVOB_hError * perror = NULL;

            switch (job_type) {

                case CVLT_BROWSE:
                    return (std::error_code (EINVAL, std::generic_category()));

                case CVLT_FULL_BACKUP:
                    jt = CVOB_JobType_Backup;  
                    options |= CVOB_FULLBKP;
                    break;

                case CVLT_INCR_BACKUP:
                    jt = CVOB_JobType_Backup;  
                    break;

                case CVLT_RESTORE:
                    jt = CVOB_JobType_Restore;
                    break;

                case CVLT_UNKNOWN_JOB:
                    return (std::error_code (EINVAL, std::generic_category()));
            } 

            int rc = fptrs.start_job (cvobsession, num_streams, options, jt,
                                      job_token.c_str (), &cvobjob, &job_id,
                                      &perror);   

            if (rc) {
                log_error (perror, log, fptrs, 
                           std::string ("cvlt_iopx::startjob"));
                return (std::error_code (EFAULT, std::generic_category()));
            } 

            return openarchive::success;
        }

        std::error_code cvlt_iopx::stopjob (bool success)
        {
            if (!ready) {
                return (std::error_code (EPERM, std::generic_category()));
            }

            int32_t options = CVOB_JOB_SUCCESS;
            if (!success) {
                options = CVOB_JOB_FAILED;  
            }

            CVOB_hError * perror = NULL;
            int rc = fptrs.end_job (cvobjob, &perror, options); 
            if (rc) {
                log_error (perror, log, fptrs, 
                           std::string ("cvlt_iopx::stopjob"));
                return (std::error_code (EFAULT, std::generic_category()));
            } 

            return openarchive::success;
        }    

        void cvlt_iopx::cvuuid_to_v4uuid (std::string &inp, std::string &outp)
        {
            /*
             * Commvault generated UUID will be in 32 byte format instead 
             * of the standard 36 byte format. We will convert from 32 byte
             * format to 36 byte format, so that regular libuuid functions
             * can process it.
             */   
            outp = inp.substr (0, 8) + "-" + inp.substr (8, 4) + "-" +
                   inp.substr (12, 4) + "-" + inp.substr (16, 4) + "-" +
                   inp.substr (20, 12);

            return;
        }

        void cvlt_iopx::v4uuid_to_cvuuid (uuid_t uid, std::string &outp)
        {
            outp.clear (); 
            char buff[4];
            for(uint32_t count = 0; count < sizeof (uuid_t); count++) {
                sprintf (buff, "%02x", uid[count]);
                outp.append (buff);
            }

            return;
        } 
           
        cvlt_cbk_context * cvlt_iopx::alloc_ctx (uint64_t seq, char *buffptr, 
                                                 size_t bufflen, off_t offset,
                                                 bool async_io, cvlt_stream *s)
        {
            cvlt_cbk_context * ctx = ctx_pool.alloc_obj ();
            if (ctx) {
                ctx->seq = seq;
                ctx->buffptr = buffptr;
                ctx->bufflen = bufflen;
                ctx->buff_offset = 0;
                uuid_clear (ctx->uuid);
                ctx->file_offset = offset;
                ctx->file_size = 0;
                ctx->ret = 0;
                ctx->err = 0;
                ctx->cbk_done.store (false); 
                ctx->iopx = this;
                ctx->stream = s;
                ctx->async_io = async_io; 
            }

            return ctx;
        }
         
        void cvlt_iopx::release_ctx (cvlt_cbk_context * ctx)
        {
            if (ctx) {
                ctx_pool.release_obj (ctx);
            }

            return; 
        }

        std::error_code cvlt_iopx::receive_data (uint64_t seq, 
                                                 std::string &cvguid, 
                                                 char * buffptr,
                                                 size_t bufflen, 
                                                 off_t offset,
                                                 bool async_io,
                                                 int64_t &ret)
        {

            /*
             * Allocate a stream to be used for restoring the file.
             */
            cvlt_stream * stream = alloc_stream (cvguid);
            if (!stream) {
                /*
                 * Failed to allocate a stream
                 */
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to allocate stream for file "
                               << cvguid;
                return (std::error_code (ENOSR, std::generic_category()));
            }

            /*
             * Initialize the callback context
             */ 

            cvlt_cbk_context * context = alloc_ctx (seq, buffptr, bufflen, 
                                                    offset, async_io, stream);
            if (!context) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find cvlt callback ctx for file "
                               << cvguid;
                release_stream (stream);
                return (std::error_code (ENOENT, std::generic_category()));
            }

            CVOB_RestoreCallbackInfo_t cbkinfo;
            cbkinfo.onHeader = cvheadercbk;
            cbkinfo.onMetadata = cvmetadatacbk; 
            cbkinfo.onData = cvdatacbk;
            cbkinfo.onEof = cveof; 

            /*
             * Start the restore
             */    
            CVOB_hError * perror = NULL;
            int rc = fptrs.restore_object (cvobjob, context, cvguid.c_str(), 
                                           offset, bufflen, &cbkinfo, &perror);
            if (rc) {
                log_error (perror, log, fptrs, 
                           std::string ("cvlt_stream::receive_data"));
                release_stream (stream);
                return (std::error_code (EFAULT, std::generic_category()));
            }

            /*
             * In case of synchronous FOP we will keep waiting for the data here.
             * We expect to be woken up by the callback handler.
             */

            if (!async_io) {

                context->sem.wait ();
                ret = context->ret;
                if (ret < 0) {
                    return std::error_code (context->err, std::generic_category());
                }
            
                release_stream (stream);
                /*
                 * Release the ctx back to pool.
                 */
                release_ctx (context);

            }

            return openarchive::success;
        }

        void cvlt_iopx::run_cbk (cvlt_cbk_context *ctx)
        {
            /*
             * Extract the request corresponding to this callback context.
             */
            req_ptr_t req;
            if (!request_map.extract (ctx->seq, req)) {
                /*
                 * Failed to find an entry in the request map.
                 */
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find an entry in request map for "
                               << ctx->cvguid;
                assert (1==0);
            }

            /*
             * Update size of the file with value that was set in ctx object.
             */
            req->get_fptr ()->set_file_size (ctx->file_size);

            if (log_level >= openarchive::logger::level_debug_2) {

                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " setting size of " << ctx->cvguid << " to "
                               << ctx->file_size << " ret : " << ctx->ret;
            }  
 
            if (ctx->async_io) {

                ssize_t ret = ctx->ret;
                ret -= ctx->file_offset;
                if (ret < 0) {
                    ret = 0;
                }

                req->set_ret (ret);

                /*
                 * Invoke the parent pread callback handler.
                 */   
                pread_cbk (req->get_fptr (), req, 
                           std::error_code (ctx->err, std::generic_category ()));

                /*
                 * Drop the request from the map.
                 */
                request_map.erase (ctx->seq);

                /*
                 * Release the ctx back to pool.
                 */
                release_stream (ctx->stream);
                release_ctx (ctx);

            } else {
                
                /*
                 * Wake up the thread waiting for data.
                 */ 
                ctx->sem.post ();

            } 

            return;
        }

        cvlt_stream * cvlt_iopx::alloc_stream (std::string &path)
        {  
            cvlt_stream * stream = cvstreammgr->alloc_stream ();
            if (!stream) {
                /*
                 * Failed to allocate a stream
                 */
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to allocate stream for " << path;

                return stream;
            }

            if (stream->get_active ()) {
                /*
                 * The stream is being currently used by another file.
                 */
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " stream allocated to " << path
                               << " is already being used by another file";

                cvstreammgr->release_stream (stream);
                return NULL; 
            }

            stream->set_active (true);
            return stream;
        }

        void cvlt_iopx::release_stream (cvlt_stream * stream)
        {
            stream->set_active (false); 
            cvstreammgr->release_stream (stream);

            return;
        }

        void cvlt_iopx::profile (void)
        {
            std::string stats;

            ctx_pool.getstats (stats);
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << stats;
            }

            return;
        } 

        void cvlt_iopx::set_cvlt_logging (void)
        {
            std::string dir = openarchive::cfgparams::get_log_dir();
            std::string log_file = "cvlt_openbackup";
            int32_t log_level = 3;
 
            if (fptrs.enable_logging (dir.c_str(), log_file.c_str(), 
                                      log_level) < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to initialize cvlt"
                               << " openbackup logging";
            }
        }

        cvlt_stream::cvlt_stream (CVOB_hJob *job, libcvob_fops_t & fops): 
                                  pjob (job), pstream (NULL), fptrs (fops), 
                                  busy(false), active (false)
        {
            log_level = openarchive::cfgparams::get_log_level();

            if (!job) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << "Invalid job object";
                return;
            } 

            /*
             * Start a stream
             */
            CVOB_hError * perror = NULL;
            int rc = fptrs.start_stream (job, &pstream, &perror); 
            if (rc) {
                log_error (perror, log, fptrs, 
                           std::string ("cvlt_stream::cvlt_stream"));
                if (pstream) {
                    release_stream ();
                } 
                return;
            } 
        }

        cvlt_stream::~cvlt_stream (void)
        {
            /*
             * Stop a stream
             */
            release_stream ();
        }

        void cvlt_stream::release_stream (void)
        {
            /*
             * Stop a stream
             */
            if (pstream) {
                CVOB_hError * perror = NULL;
                int rc = fptrs.end_stream (pstream, &perror); 
                if (rc) {
                    log_error (perror, log, fptrs, 
                               std::string ("cvlt_stream::cvlt_stream"));
                }
                pstream = NULL;
            }
        }

        std::error_code cvlt_stream::alloc_item (std::string &path,
                                                 std::string &uuid,
                                                 size_t len) 
        { 
            char guid[256];
            CVOB_hError * perror = NULL;

            int rc = fptrs.send_item_begin (pstream, &item, path.c_str(), 
                                            CVOB_ItemType_file, uuid.c_str(),
                                            guid, len, NULL, &perror);

            if (rc) {
                log_error (perror, log, fptrs, 
                           std::string ("cvlt_stream::alloc_item"));
                return (std::error_code (EFAULT, std::generic_category()));
            }

            cvguid = guid;    

            if (log_level >= openarchive::logger::level_debug_2) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                               << " path: " << path << " uuid: " <<uuid 
                               << " cvlt uuid: " << cvguid;
            }

            return openarchive::success;
        }

        std::error_code cvlt_stream::release_item (void)
        {
            CVOB_hError * perror = NULL;

            int rc = fptrs.send_end (pstream, item, CVOB_ItemStatus_Good, "",
                                     &perror);
            if (rc) {
                log_error (perror, log, fptrs, 
                           std::string ("cvlt_stream::release_item"));
                return (std::error_code (EFAULT, std::generic_category()));
            }

            return openarchive::success;
        }

        std::error_code cvlt_stream::send_metadata (uint32_t id, char *buff, 
                                                    size_t bufflen)
        {
            CVOB_hError * perror = NULL;

            int rc = fptrs.send_metadata (pstream, item, id, 
                                          (const char *) buff, bufflen, 
                                          &perror);
            if (rc) {
                log_error (perror, log, fptrs, 
                           std::string ("cvlt_stream::send_metadata"));
                return (std::error_code (EFAULT, std::generic_category()));
            }

            return openarchive::success;
        }

        std::error_code cvlt_stream::send_data (char *buffptr, size_t bufflen)
        {
            CVOB_hError * perror = NULL;

            int rc = fptrs.send_data (pstream, item, buffptr, bufflen, 
                                      &perror);
            if (rc) {
                log_error (perror, log, fptrs, 
                           std::string ("cvlt_stream::send_data"));
                return (std::error_code (EFAULT, std::generic_category()));
            }

            return openarchive::success;
        }

 
        cvlt_stream_manager::cvlt_stream_manager (CVOB_hJob *job, 
                                                  uint32_t streams, 
                                                  libcvob_fops_t & fops,
                                                  uint32_t num_worker_threads):
                                                  pjob (job), 
                                                  num_streams (0),
                                                  enable_reserve (false),
                                                  valid (false),
                                                  sem (NULL),
                                                  fptrs (fops)
        {
            /*
             * Start allocating the required number of streams
             */
            for (uint32_t count = 0; count < streams; count++) {
                 cvlt_stream * pstream = new cvlt_stream (pjob, fops);
                 if (pstream && pstream->get ()) {
                     num_streams++;
                     queue_streams.push (pstream);  
                } else {
                    if (pstream) {
                        delete pstream;
                    }  
                 }
            }  

            /*
             * All the required number of streams have been allocated.
             */ 
            sem = new openarchive::arch_core::semaphore (num_streams);
            if (!sem) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << "Failed to allocate semaphore";
                return;
            }

            /*
             * Check whether the number of streams equals the number of worker
             * threads. If not then we will have to enable stream reservation.
             */    

            if (num_streams != num_worker_threads) {

                enable_reserve = true;

                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " Number of streams : " << num_streams
                               << " Number of worker threads : " 
                               << num_worker_threads 
                               << " Stream reservation will be enabled";
            }  

            valid = true;
        }   

        cvlt_stream_manager::~cvlt_stream_manager (void)
        {
            /*
             * Release all the streams.
             */
            cvlt_stream * pstream = NULL;
              
            while(queue_streams.pop (pstream)) {
                delete pstream;
            }
                    
        }
           
        cvlt_stream * cvlt_stream_manager::get_free_stream (void)
        {
            cvlt_stream * pstream = NULL;
            bool found = false;
              
            while(queue_streams.pop (pstream)) {
                if (pstream) {
                    if (false == pstream->get_busy()) {
                        /*
                         * Found a free stream.
                         */
                        pstream->set_busy(true);
                        found = true;
                    }
                    queue_streams.push (pstream);
                }

                if (found) {
                    return pstream;  
                } 
            }

            return NULL;
        } 

        cvlt_stream * cvlt_stream_manager::alloc_stream (void)
        {
            if (!valid) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " Streams not available";
                return NULL;
            }

            /*
             * Check whether stream reservtaion is required for the existing
             * configuration.
             */ 
            if (enable_reserve) {
                /*
                 * Wait until a free stream is available.
                 */
                sem->wait (); 
                cvlt_stream * ptr = NULL;
                if (queue_streams.pop (ptr)) {
                    return ptr;  
                }

                return NULL;

            } else {
                /*
                 * No stream reservation. Check the TLS to get a stream.
                 */
                tls_ref_t tls_ref = openarchive::arch_tls::get_arch_tls ();
                cvlt_stream * ptr = tls_ref->get_stream_ptr ();
                if (!ptr) {
                    ptr = get_free_stream ();
                    tls_ref->set_stream_ptr (ptr);
                } 

                return ptr; 

            }

        }
        
        void cvlt_stream_manager::release_stream (cvlt_stream *stream)
        {
            if (!valid) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " Streams not available";
                return;
            }

            if (!enable_reserve) {
                /*
                 * Nothing needs to be done.
                 */
                return;
            }

            if (queue_streams.push (stream)) {
                /*
                 * Increase count of available streams
                 */
                sem->post (); 
            }

            return;
        }
 
        ssize_t cvmdserialize (cvmd & md, char * bufptr, size_t bufsize)
        {
            ssize_t ret = 0;

            if (bufsize < sizeof (cvmd)) {
                return -1;
            }

            memcpy (bufptr, md.uuid, sizeof (uuid_t));
            ret += sizeof (uuid_t);

            uint64_t val = htole64 (md.file_len);
            memcpy (bufptr + sizeof (uuid_t), &val, sizeof (val));   
            ret += sizeof (val);

            return ret;
        }

        ssize_t cvmdunserialize (cvmd & md, char * bufptr, size_t bufsize)
        {
            ssize_t ret = 0;  

            if (bufsize < sizeof (uuid_t)) {
                return -1; 
            }

            memcpy (md.uuid, bufptr, sizeof (uuid_t));
            ret += sizeof (uuid_t);  

            if (bufsize < (ret + sizeof (uint64_t))) {
                return -1; 
            }

            uint64_t val;
            memcpy (&val, bufptr + sizeof (uuid_t), sizeof (val));   
            md.file_len = le64toh (val);
            ret += sizeof (val);
             
            return ret;
             
        }

        int32_t cvheadercbk (void * context, const char * path, 
                             const char * guid, CVOB_ItemType type,
                             CVOB_BackupItemAttributes_t * attr, 
                             int64_t size)
        {
            struct cvlt_cbk_context * ctx = (struct cvlt_cbk_context *) context;
            static src::severity_logger<int> log;
            static int32_t log_level = openarchive::cfgparams::get_log_level ();

            /*
             * Fill the needed information in the context structure.
             */
            ctx->path = path;
            uuid_parse (guid, ctx->uuid);

            if (log_level >= openarchive::logger::level_debug_2) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " size : " << size << " path : " << path
                               << " guid : " << guid;
            }   

            return 0;
            
        }
 
        int32_t cvmetadatacbk (void *context, CVOB_MetadataID id, char * buffer,
                               size_t buff_size)
        {
            struct cvlt_cbk_context * ctx = (struct cvlt_cbk_context *) context;
            static src::severity_logger<int> log;
            static int32_t log_level = openarchive::cfgparams::get_log_level ();

            /*
             * Parse the metadata buffer and populate the required fields
             */ 
            if (ctx->ret < 0) {
                /*
                 * Error occured during earlier callback processing
                 */
                if (!ctx->cbk_done.load ()) {
                    ctx->cbk_done.store (true); 
                    cvcbk (ctx);
                }
                return 0;   
            }

            cvmd md;
            if (cvmdunserialize (md, buffer, buff_size) < 0) {
                /*
                 * Failed to parse metadata
                 */
                ctx->ret = -1;
                ctx->err = EILSEQ; 

                if (!ctx->cbk_done.load ()) {
                    ctx->cbk_done.store (true); 
                    cvcbk (ctx);
                }

                return 0;  
            }

            ctx->file_size = md.file_len; 

            if (log_level >= openarchive::logger::level_error) {
                char uuid[40];
                uuid_unparse (md.uuid, uuid);
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " file size : " << ctx->file_size
                               << " app guid : " << uuid;
            }   

            return 0;
        }
 
        int32_t cvdatacbk (void *context, char *buffer, size_t buff_size)
        {
            struct cvlt_cbk_context * ctx = (struct cvlt_cbk_context *) context;
            static src::severity_logger<int> log;
            static int32_t log_level = openarchive::cfgparams::get_log_level ();

            if (ctx->ret < 0) {
                /*
                 * Error occured during earlier callback processing
                 */
                if (!ctx->cbk_done.load ()) {
                    ctx->cbk_done.store (true); 
                    cvcbk (ctx);
                }
                return 0;   
            }

            /*
             * Copy the data to passed in buffer
             */
            char * tgtbuff = NULL;
            size_t tgtbufflen = 0;

            tgtbuff = ctx->buffptr;
            tgtbufflen = ctx->bufflen;


            if (log_level >= openarchive::logger::level_debug_2) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " offset : " << ctx->buff_offset
                               << " size : " << buff_size
                               << " bufflen : " << tgtbufflen;
            }   

            if ((ctx->buff_offset + buff_size) > tgtbufflen) {

                ctx->ret = -1;
                ctx->err = ENOBUFS;

                /*
                 * Invoke the callback handler
                 */
                if (!ctx->cbk_done.load ()) {
                    ctx->cbk_done.store (true); 
                    cvcbk (ctx);
                }

            } else {

                memcpy (tgtbuff+ctx->buff_offset, buffer, buff_size);
                ctx->buff_offset += buff_size;

            }

            return 0;
        }
  
        int32_t cveof (void *context, int32_t ret)
        {
            struct cvlt_cbk_context * ctx = (struct cvlt_cbk_context *) context;
            static src::severity_logger<int> log;
            static int32_t log_level = openarchive::cfgparams::get_log_level ();

            if (log_level >= openarchive::logger::level_debug_2) {

                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " context : " << std::hex
                               << (uint64_t) context << std::dec 
                               << " cvlt ret : " << ret
                               << " ctx ret : " << ctx->ret;
            }   

            if (ctx->ret < 0) {
                /*
                 * Error occured during earlier callback processing
                 */
                if (!ctx->cbk_done.load ()) {
                    ctx->cbk_done.store (true); 
                    cvcbk (ctx);
                }
                return 0;   
            }

            if (ret < 0) {
                ctx->ret = -1;
                ctx->err = EIO; 
            } else { 
                ctx->ret = ctx->buff_offset;
                ctx->err = 0; 
            
                /*
                 * Update the file size. We will always be restoring full 
                 * file irrespective of the request size.
                 */
                ctx->file_size = ctx->ret; 
            }

            /*
             * Invoke the callback handler
             */
            if (!ctx->cbk_done.load ()) {
                ctx->cbk_done.store (true); 
                cvcbk (ctx);
            }

            return 0;
        }

        void cvcbk (cvlt_cbk_context *ctx)
        {
            cvlt_iopx * iopx = ctx->iopx; 
            iopx->run_cbk (ctx);

            return;
        }
    }
}

