/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <gfapi_iopx.h>

namespace bp = ::boost::process;
#define GLUSTERFIND "glusterfind"
#define SESSION_NAME "openarchive"

namespace openarchive
{
    namespace gfapi_iopx
    {
        const std::string gfapilib = "libgfapi.so";
        const std::string gfidattr = "glusterfs.gfid.string";
 
        gfapi_iopx::gfapi_iopx (std::string name, io_service_ptr_t svc,
                                std::string vol): 
                                openarchive::arch_iopx::arch_iopx (name, svc),
                                volume(vol), hname("/var/run/glusterd.socket"), 
                                protocol("unix"), port(0), fops(gfapilib), 
                                fptrs(fops.get_fops()), glfs(NULL), ready(false)
        {

            log_level = openarchive::cfgparams::get_log_level();

            if (fptrs.gl_new) {
                glfs = fptrs.gl_new (volume.c_str());
            }
       
            if (!glfs) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to allocate glfs_t instance"
                               << " gl_new fop @  " << fptrs.gl_new;

                return;
 
            }


            std::string vol_file = std::string("/var/lib/glusterd/vols/") +
                                   volume + "/trusted-" + volume + ".gfapi.vol";

            /*
             * Check whether the GFAPI specific vol file exists. If so we will
             * use it. Else we will connect to the glusterd running on the 
             * server.
             */ 
            bool connected = false;
            struct stat sinfo;
            if (!::stat (vol_file.c_str (), &sinfo) && S_ISREG (sinfo.st_mode)) {
                
                /* 
                 * Vol file exists. Use it.
                 */
                if (fptrs.gl_set_volfile) {
                    if (fptrs.gl_set_volfile (glfs, vol_file.c_str())) {
                        BOOST_LOG_FUNCTION ();
                        BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                       << " failed to connect to glusterd using"
                                       << " volfile @ " <<vol_file;
                    } else {
                        BOOST_LOG_FUNCTION ();
                        BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                       << " connected to glusterd using"
                                       << " volfile @ " <<vol_file;
                        connected = true; 
                    }  
                } 
            } 

            if (!connected) {
                if (!fptrs.gl_set_volfile_server) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " failed to find glfs_set_volfile_server "
                                   << "entry point";

                    return;
                }

                if (fptrs.gl_set_volfile_server (glfs, protocol.c_str(), 
                                                 hname.c_str(), port)) {

                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " failed to connect to glusterd through"
                                   << " unix domain socket";

                    return;

                }
            }

            std::string logdir = openarchive::cfgparams::get_log_dir();
            gflogpath = logdir+"/gfapi-iopx.log";
            gfloglevel = 7;

            if (!fptrs.gl_set_logging) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_set_logging entry";

                return;
            }

            if (fptrs.gl_set_logging (glfs, gflogpath.c_str(), gfloglevel)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to initialize logging";

                return;
            }

            if (!fptrs.gl_init) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_init entry point";

                return;
            }

            int ret=-1;
            for(int count=0; count<3; count++) {
                ret = fptrs.gl_init (glfs);
                if (!ret) {
                    break;
                }

                /*
                 * Wait for sometime and reattempt the initialization.
                 */
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }

            if (ret) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to initialize glfs_t";
                return;
            }

            ready = true;

            if (log_level >= openarchive::logger::level_debug_2) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                               << " Successfully initialized glfs instance";
            }

            return; 
            
        }

        gfapi_iopx::~gfapi_iopx(void)
        {
            while (get_refcount () > 0 ) {
                /*
                 * Wait until there are no more active references
                 */ 
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }

            if (glfs) {

                if (!fptrs.gl_fini) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " failed to find glfs_fini entry point";

                    return;
                }

                if (fptrs.gl_fini (glfs)) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " cleanup failed for glfs instance";

                    return;
                }

            } 

            glfs = NULL;
        } 
 
        std::error_code gfapi_iopx::open (file_ptr_t fp, req_ptr_t req)
        {
            glfs_fd_t * glfd = NULL;
          
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_open) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_open entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            if (!fptrs.gl_creat) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_creat entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            if (req->get_flags() & O_CREAT) {
                glfd = fptrs.gl_creat (glfs, 
                                       fp->get_loc().get_pathstr().c_str(),
                                       req->get_flags(), 
                                       req->get_len()); 
            } else {
                glfd = fptrs.gl_open (glfs, 
                                      fp->get_loc().get_pathstr().c_str(),
                                      req->get_flags()); 
            } 

            if (!glfd) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to open file: "
                               << fp->get_loc().get_pathstr()
                               << " flags: " << req->get_flags()
                               << " error: " << strerror(errno);  

                return (std::error_code (errno, std::generic_category()));
            }

            /*
             * The file has been opened successfully. Increase the ref count 
             * for glfs_t instance object. 
             */ 
            get();

            /*
             * Save the fd for future fops.
             */ 
            file_info_t info;
            info.set_glfd (glfd); 
            fp->set_file_info (get_name(), info);

            if (log_level >= openarchive::logger::level_debug_2) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                               << " Successfully opened file: "
                               << fp->get_loc().get_pathstr();
            }

            return openarchive::success;
        }

        std::error_code gfapi_iopx::close (file_ptr_t fp, req_ptr_t req)
        {
            glfs_fd_t * glfd = NULL;
          
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_close) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_close entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            file_info_t info;
            if (!fp->get_file_info (get_name(), info)) {

                if (log_level >= openarchive::logger::level_debug_2) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                                   << " file must have been closed already as"
                                   << " failed to find glusterfs fd info"
                                   << " for " << get_name() << " iopx for file "
                                   << fp->get_loc().get_pathstr();
                }

                return openarchive::success;

            }
 
            glfd = info.get_glfd (); 
            if (!glfd) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glusterfs fd invalid  for " << get_name()
                               << "iopx for file "
                               << fp->get_loc().get_pathstr();

                return (std::error_code (ENXIO, std::generic_category()));
            }
        
            fptrs.gl_close (glfd);

            /*
             * Decrease the ref count for glfs_t instance object. 
             */ 
            put();

            /*
             * Drop the fd from map.
             */
            fp->erase_file_info (get_name());
  
            if (log_level >= openarchive::logger::level_debug_2) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                               << " Successfully closed file: "
                               << fp->get_loc().get_pathstr();
            }

            return openarchive::success;
        }

        std::error_code gfapi_iopx::close (file_t & fp)
        {
            glfs_fd_t * glfd = NULL;
          
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_close) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_close entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            file_info_t info;
            if (!fp.get_file_info (get_name(), info)) {

                if (log_level >= openarchive::logger::level_debug_2) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                                   << " file must have been closed already as"
                                   << " failed to find glusterfs fd info"
                                   << " for " << get_name() << " iopx for file "
                                   << fp.get_loc().get_pathstr();
                }

                return openarchive::success;

            }
 
            glfd = info.get_glfd (); 
            if (!glfd) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glusterfs fd invalid  for " << get_name()
                               << "iopx for file "
                               << fp.get_loc().get_pathstr();

                return (std::error_code (ENXIO, std::generic_category()));
            }
        
            fptrs.gl_close (glfd);

            /*
             * Decrease the ref count for glfs_t instance object. 
             */ 
            put();

            /*
             * Drop the fd from map.
             */
            fp.erase_file_info (get_name());
  
            if (log_level >= openarchive::logger::level_debug_2) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                               << " Successfully closed file: "
                               << fp.get_loc().get_pathstr();
            } 

            return openarchive::success;
        }

        std::error_code gfapi_iopx::pread (file_ptr_t fp, req_ptr_t req)
        {
            glfs_fd_t * glfd = NULL;
          
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_pread) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_pread entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            file_info_t info;
            if (!fp->get_file_info (get_name(), info)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glusterfs fd info "
                               << "for " << get_name() << "iopx for file "
                               << fp->get_loc().get_pathstr();

                return (std::error_code (ENOENT, std::generic_category()));
            }
 
            glfd = info.get_glfd (); 
            if (!glfd) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glusterfs fd invalid  for " << get_name()
                               << "iopx for file "
                               << fp->get_loc().get_pathstr();

                return (std::error_code (ENXIO, std::generic_category()));
            }

            void *buff = NULL;
            buff = openarchive::iopx_req::get_buff_baseaddr (req);

            assert (buff != NULL);

            /*
             * Now we will perform pread on the extracted gluster fd.
             */
            ssize_t ret = fptrs.gl_pread (glfd, buff, req->get_len (), 
                                          req->get_offset (), 
                                          req->get_flags ());

            req->set_ret (ret);

            if (ret < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " pread failed for file " 
                               << fp->get_loc().get_pathstr()
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);

                return (std::error_code (errno, std::generic_category()));
            }

            if (log_level >= openarchive::logger::level_debug_2) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                               << " Successfully read " << ret << " bytes from "
                               << fp->get_loc().get_pathstr() <<" at offset "
                               << req->get_offset () ;
            }

            return openarchive::success;
        }

        std::error_code gfapi_iopx::pwrite (file_ptr_t fp, req_ptr_t req)
        {
            glfs_fd_t * glfd = NULL;
          
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_pwrite) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_pwrite entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            file_info_t info;
            if (!fp->get_file_info (get_name(), info)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glusterfs fd info "
                               << "for " << get_name() << "iopx for file "
                               << fp->get_loc().get_pathstr();

                return (std::error_code (ENOENT, std::generic_category()));
            }
 
            glfd = info.get_glfd (); 
            if (!glfd) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glusterfs fd invalid  for " << get_name()
                               << "iopx for file "
                               << fp->get_loc().get_pathstr();

                return (std::error_code (ENXIO, std::generic_category()));
            }

            void *buff = NULL;
            buff = openarchive::iopx_req::get_buff_baseaddr (req);

            assert (buff != NULL);
 
            /*
             * Now we will perform pwrite on the extracted gluster fd.
             */
            ssize_t ret = fptrs.gl_pwrite (glfd, buff, req->get_len (), 
                                           req->get_offset (), 
                                           req->get_flags ());

            req->set_ret (ret);

            if (ret < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " pwrite failed for file " 
                               << fp->get_loc().get_pathstr()
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);

                return (std::error_code (errno, std::generic_category()));
            }

            if (log_level >= openarchive::logger::level_debug_2) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                               << " Successfully wrote " << ret << " bytes to "
                               << fp->get_loc().get_pathstr() <<" at offset "
                               << req->get_offset () ;
            }

            return openarchive::success;
        }

        std::error_code gfapi_iopx::fstat (file_ptr_t fp, req_ptr_t req)
        {
            glfs_fd_t * glfd = NULL;
          
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_fstat) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_fstat entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            file_info_t info;
            if (!fp->get_file_info (get_name(), info)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glusterfs fd info "
                               << "for " << get_name() << "iopx for file "
                               << fp->get_loc().get_pathstr();

                return (std::error_code (ENOENT, std::generic_category()));
            }
 
            glfd = info.get_glfd (); 
            if (!glfd) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glusterfs fd invalid  for " << get_name()
                               << "iopx for file "
                               << fp->get_loc().get_pathstr();

                return (std::error_code (ENXIO, std::generic_category()));
            }

            struct stat * statp = NULL;
            statp = openarchive::iopx_req::get_stat_baseaddr (req);

            assert (statp != NULL);

            int ret = fptrs.gl_fstat (glfd, statp);

            if (ret < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " fstat failed for file " 
                               << fp->get_loc().get_pathstr()
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);

                return (std::error_code (errno, std::generic_category()));
            }

            return openarchive::success;
        } 

        std::error_code gfapi_iopx::stat (file_ptr_t fp, req_ptr_t req)
        {
          
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_stat) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_stat entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            struct stat * statp = NULL;
            statp = openarchive::iopx_req::get_stat_baseaddr (req);

            assert (statp != NULL);

            int ret = fptrs.gl_stat (glfs, fp->get_loc().get_pathstr().c_str(),
                                     statp);

            if (ret < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " stat failed for file " 
                               << fp->get_loc().get_pathstr()
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);

                return (std::error_code (errno, std::generic_category()));
            }

            return openarchive::success;
        } 

        std::error_code gfapi_iopx::ftruncate (file_ptr_t fp, req_ptr_t req)
        {
            glfs_fd_t * glfd = NULL;
          
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_ftruncate) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_ftruncate entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            file_info_t info;
            if (!fp->get_file_info (get_name(), info)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glusterfs fd info "
                               << "for " << get_name() << "iopx for file "
                               << fp->get_loc().get_pathstr();

                return (std::error_code (ENOENT, std::generic_category()));
            }
 
            glfd = info.get_glfd (); 
            if (!glfd) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glusterfs fd invalid  for " << get_name()
                               << "iopx for file "
                               << fp->get_loc().get_pathstr();

                return (std::error_code (ENXIO, std::generic_category()));
            }

            off_t length = req->get_len (); 

            int ret = fptrs.gl_ftruncate (glfd, length);

            if (ret < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " ftruncate failed for file " 
                               << fp->get_loc().get_pathstr()
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);

                return (std::error_code (errno, std::generic_category()));
            }

            return openarchive::success;
        }

        std::error_code gfapi_iopx::truncate (file_ptr_t fp, req_ptr_t req)
        {
          
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_truncate) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_truncate entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            off_t length = req->get_len (); 

            int ret = fptrs.gl_truncate (glfs, 
                                         fp->get_loc().get_pathstr().c_str(), 
                                         length);

            if (ret < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " ftruncate failed for file " 
                               << fp->get_loc().get_pathstr()
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);

                return (std::error_code (errno, std::generic_category()));
            }

            return openarchive::success;
        }
        
        std::error_code gfapi_iopx::fsetxattr (file_ptr_t fp, req_ptr_t req)
        {
            glfs_fd_t * glfd = NULL;
          
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_fsetxattr) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_fsetxattr entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            file_info_t info;
            if (!fp->get_file_info (get_name(), info)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glusterfs fd info "
                               << "for " << get_name() << "iopx for file "
                               << fp->get_loc().get_pathstr();

                return (std::error_code (ENOENT, std::generic_category()));
            }
 
            glfd = info.get_glfd (); 
            if (!glfd) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glusterfs fd invalid  for " << get_name()
                               << "iopx for file "
                               << fp->get_loc().get_pathstr();

                return (std::error_code (ENXIO, std::generic_category()));
            }

            void *buff = NULL;
            buff = openarchive::iopx_req::get_xtattr_baseaddr (req);

            assert (buff != NULL);

            int ret = fptrs.gl_fsetxattr (glfd, req->get_desc().c_str(),
                                          buff, req->get_len(),
                                          req->get_flags());

            if (ret < 0) {
                if ((EEXIST != errno) ||
                    (log_level >= openarchive::logger::level_debug_2)) { 
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " fsetxattr failed for file " 
                                   << fp->get_loc().get_pathstr()
                                   << " attribute: " << req->get_desc() 
                                   << " error code: " << errno
                                   << " error desc: " << strerror(errno);
                }

                return (std::error_code (errno, std::generic_category()));
            }

            return openarchive::success;
        }

        std::error_code gfapi_iopx::setxattr (file_ptr_t fp, req_ptr_t req)
        {
          
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_setxattr) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_setxattr entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            void *buff = NULL;
            buff = openarchive::iopx_req::get_xtattr_baseaddr (req);

            assert (buff != NULL);

            int ret = fptrs.gl_setxattr (glfs, 
                                         fp->get_loc().get_pathstr().c_str(),
                                         req->get_desc().c_str(),
                                         buff, req->get_len(), 
                                         req->get_flags());

            if (ret < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " setxattr failed for file " 
                               << fp->get_loc().get_pathstr()
                               << " attribute: " << req->get_desc() 
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);

                return (std::error_code (errno, std::generic_category()));
            }

            return openarchive::success;
        }

        std::error_code gfapi_iopx::fgetxattr (file_ptr_t fp, req_ptr_t req)
        {
            glfs_fd_t * glfd = NULL;
          
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_fgetxattr) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_fgetxattr entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            file_info_t info;
            if (!fp->get_file_info (get_name(), info)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glusterfs fd info "
                               << "for " << get_name() << "iopx for file "
                               << fp->get_loc().get_pathstr();

                return (std::error_code (ENOENT, std::generic_category()));
            }
 
            glfd = info.get_glfd (); 
            if (!glfd) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glusterfs fd invalid  for " << get_name()
                               << "iopx for file "
                               << fp->get_loc().get_pathstr();

                return (std::error_code (ENXIO, std::generic_category()));
            }

            void *buff = NULL;
            buff = openarchive::iopx_req::get_xtattr_baseaddr (req);

            int ret = fptrs.gl_fgetxattr (glfd, req->get_desc().c_str(),
                                          buff, req->get_len());

            if (ret < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " fgetxattr failed for file " 
                               << fp->get_loc().get_pathstr()
                               << " attribute: " << req->get_desc() 
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);

                return (std::error_code (errno, std::generic_category()));
            }

            req->set_ret (ret);

            return openarchive::success;
        }

        std::error_code gfapi_iopx::getxattr (file_ptr_t fp, req_ptr_t req)
        {
          
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_getxattr) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_getxattr entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            void *buff = NULL;
            buff = openarchive::iopx_req::get_xtattr_baseaddr (req);

            int ret = fptrs.gl_getxattr (glfs, 
                                         fp->get_loc().get_pathstr().c_str(),
                                         req->get_desc().c_str(),
                                         buff, req->get_len()); 

            if (ret < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " getxattr failed for file " 
                               << fp->get_loc().get_pathstr()
                               << " attribute: " << req->get_desc() 
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);

                return (std::error_code (errno, std::generic_category()));
            }

            req->set_ret (ret);

            return openarchive::success;
        }

        std::error_code gfapi_iopx::fremovexattr (file_ptr_t fp, req_ptr_t req)
        {
            glfs_fd_t * glfd = NULL;
          
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_fremovexattr) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_fremovexattr entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            file_info_t info;
            if (!fp->get_file_info (get_name(), info)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glusterfs fd info "
                               << "for " << get_name() << "iopx for file "
                               << fp->get_loc().get_pathstr();

                return (std::error_code (ENOENT, std::generic_category()));
            }
 
            glfd = info.get_glfd (); 
            if (!glfd) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glusterfs fd invalid  for " << get_name()
                               << "iopx for file "
                               << fp->get_loc().get_pathstr();

                return (std::error_code (ENXIO, std::generic_category()));
            }

            int ret = fptrs.gl_fremovexattr (glfd, req->get_desc().c_str());

            if (ret < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " fremovexattr failed for file " 
                               << fp->get_loc().get_pathstr()
                               << " attribute: " << req->get_desc() 
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);

                return (std::error_code (errno, std::generic_category()));
            }

            return openarchive::success;
        }

        std::error_code gfapi_iopx::removexattr (file_ptr_t fp, req_ptr_t req)
        {
          
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_removexattr) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_removexattr entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            int ret = fptrs.gl_removexattr (glfs, 
                                            fp->get_loc().get_pathstr().c_str(),
                                            req->get_desc().c_str());

            if (ret < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " removexattr failed for file " 
                               << fp->get_loc().get_pathstr()
                               << " attribute: " << req->get_desc() 
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);

                return (std::error_code (errno, std::generic_category()));
            }

            return openarchive::success;
        }

        std::error_code gfapi_iopx::lseek (file_ptr_t fp, req_ptr_t req)
        {
            glfs_fd_t * glfd = NULL;
          
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_lseek) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_lseek entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            file_info_t info;
            if (!fp->get_file_info (get_name(), info)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glusterfs fd info "
                               << "for " << get_name() << "iopx for file "
                               << fp->get_loc().get_pathstr();

                return (std::error_code (ENOENT, std::generic_category()));
            }
 
            glfd = info.get_glfd (); 
            if (!glfd) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glusterfs fd invalid  for " << get_name()
                               << "iopx for file "
                               << fp->get_loc().get_pathstr();

                return (std::error_code (ENXIO, std::generic_category()));
            }
            
            off_t offset = req->get_offset();
            int whence = req->get_flags();
 
            int ret = fptrs.gl_lseek (glfd, offset, whence);

            if (ret < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " lseek failed for file " 
                               << fp->get_loc().get_pathstr()
                               << " offset: " << offset << " whence: " <<whence
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);

                return (std::error_code (errno, std::generic_category()));
            }

            return openarchive::success;
        }

        std::error_code gfapi_iopx::mkdir (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_mkdir) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_mkdir entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            int ret = fptrs.gl_mkdir(glfs, 
                                     fp->get_loc().get_pathstr().c_str(),
                                     req->get_flags());

            if (ret < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " mkdir failed for file " 
                               << fp->get_loc().get_pathstr()
                               << " flags " << req->get_flags() 
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);

                return (std::error_code (errno, std::generic_category()));
            }

            return openarchive::success;
        }

        std::error_code gfapi_iopx::extract_gfid (std::string path, 
                                                  struct stat * statbuf,
                                                  void * buff,
                                                  uint32_t length) 
        {
            if (!statbuf || !buff || length < GFAPI_HANDLE_LENGTH) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " invalid params for " << path;
                return (std::error_code (EFAULT, std::generic_category()));
            }

            if (!fptrs.gl_h_extract_handle) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_h_extract_handle "
                               << "entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            if (!fptrs.gl_h_lookupat) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_h_lookupat entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            struct glfs_object *leaf = NULL;

            leaf = fptrs.gl_h_lookupat (glfs, NULL, path.c_str(), statbuf, 0);

            if (!leaf) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs_h_lookupat failed for file " << path
                               << " error code: " << errno
                               << " error desc: " << strerror(errno);

                return (std::error_code (errno, std::generic_category()));
            }

            int32_t ret = fptrs.gl_h_extract_handle (leaf,
                                                     (unsigned char*) buff,
                                                     GFAPI_HANDLE_LENGTH);

            if (ret != sizeof(uuid_t)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to extract handle for file " <<path 
                               << " ret: " <<ret;

                return (std::error_code (ENOENT, std::generic_category()));
            }

            return openarchive::success;
        }

        std::error_code gfapi_iopx::getuuid (file_ptr_t fp, req_ptr_t req)
        {
            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }

            void *buff = NULL;
            buff = openarchive::iopx_req::get_xtattr_baseaddr (req);

            assert (buff != NULL);
            assert (req->get_len () >= sizeof(uuid_t));

            struct stat statbuf = {0,}; 
            std::error_code ec = extract_gfid (fp->get_loc().get_pathstr(), 
                                               &statbuf, buff, req->get_len ());
            if (ec != ok) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to extract gfid for file "
                               << fp->get_loc().get_pathstr()
                               << " error code: " << ec.value ()
                               << " error desc: " << strerror(ec.value ());
                return ec;
            }

            if (log_level >= openarchive::logger::level_debug_2) {
                char uid[40];
                uuid_unparse ((unsigned char *)buff, uid);

                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                               << " Extracted uuid " <<uid << " for file " 
                               << fp->get_loc().get_pathstr();
            } 
  
            return openarchive::success;

        }

        std::error_code gfapi_iopx::resolve (file_ptr_t fp, req_ptr_t req)
        {
            /*
             * Check whether sharding is enabled on the glusterfs volume. 
             * If sharding is enabled then the passed in file can be resolved
             * to multiple shards, size of each being the shard length.
             * We will list out all the shards that correspond to the file.
             * The shard path will be of the following format.
             * .shard/<GFID>.extent where extent could be in the range of 1
             * to max_extent. If sharding is not enabled then the resolved 
             * file will be the same as original file.
             */

            std::list <arch_loc_t> * ploc = req->get_ploc (); 
            if (!ploc) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " invalid list passed for " 
                               << fp->get_loc().get_pathstr();
                return (std::error_code (EFAULT, std::generic_category()));
            }
             
            ploc->empty ();

            bool en_shard;
            std::error_code ec = sharding_enabled (fp, en_shard);
            if (ec != ok) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to determine whether sharding is "
                               << "enable for " 
                               << fp->get_loc().get_pathstr();
                return ec;
            } 
           
            if (!en_shard) {
                ploc->push_back (fp->get_loc ());
                return openarchive::success;
            } 

            uuid_t uuid;
            struct stat statbuf = {0, };
      
            ec = extract_gfid (fp->get_loc().get_pathstr (), &statbuf, uuid,
                               sizeof(uuid));
            if (ec != ok) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to extract gfid for file "
                               << fp->get_loc().get_pathstr()
                               << " error code: " << ec.value ()
                               << " error desc: " << strerror(ec.value ());
                return ec;
            }

            char gfid[40];
            uuid_unparse (uuid, gfid);

            uint64_t shard_size;
            ec = get_shard_size (fp, shard_size);
            if (ec != ok) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to determine size of shard for file "
                               << fp->get_loc().get_pathstr()
                               << " error code: " << ec.value ()
                               << " error desc: " << strerror(ec.value ());
                return ec;
            } 

            /*
             * Now that we have all the needed information like shard size 
             * and gfid, we will go ahead with determining the individual
             * shard file paths.
             */
            uint32_t num_bits = openarchive::arch_core::bitops::bit_width (shard_size);
            uint64_t remaining = statbuf.st_size & (shard_size -1);
            uint64_t num_shards = statbuf.st_size >> num_bits;
            if (remaining) {
                num_shards++; 
            }

            /*
             * Consider the following file sample which is of size 6MB and is
             * stored on a glusterfs volume where sharding is enabled, with a
             * shard size of 4MB. In this case the file would be stored as
             * sample at the intended location and another file called GFID.1
             * would be created under .shard directory. We will provide the list
             * of all those shard files.
             */
            for(uint32_t count = 1; count < num_shards; count ++)
            {
                std::string file_path = std::string (".shard/") + gfid + "." +
                                        boost::lexical_cast<std::string>(count);

                /*
                 * Check whether this is a valid file on the glusterfs volume
                 */
                struct stat statbuf = {0, };
                uuid_t uuid;
                
                std::error_code ec = extract_gfid (file_path.c_str(), &statbuf,
                                                   uuid, sizeof(uuid));

                if (ec != ok) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " failed to extract gfid for file " 
                                   << file_path.c_str ()
                                   << " error code: " << errno
                                   << " error desc: " << strerror(errno);

                    return (std::error_code (errno, std::generic_category()));
                }

                if (S_ISREG (statbuf.st_mode)) {

                    /*
                     * We found a valid shard file.
                     */

                    arch_loc_t loc = fp->get_loc ();
                    loc.set_path (file_path);
                    loc.set_uuid (uuid); 

                    ploc->push_back (loc); 
 
                } 
            }

            return openarchive::success;
        } 
        
        std::error_code gfapi_iopx::sharding_enabled (file_ptr_t fp, bool &en)
        {
            arch_loc_t loc = fp->get_loc ();

            /*
             * Check whether sharding is currently enabled on the volume by 
             * extracting the volume property.
             */
            std::string cmd = std::string("gluster volume get ") + 
                             loc.get_store () + 
                             " features.shard | tail -n1 | awk \'{print $2}\'";
            
            openarchive::arch_core::popen process (cmd, 100);

            if (process.is_valid ()) {

                std::vector<std::string> & vec = process.get_output ();
                if (vec.size()) {

                    if (vec[0] == "off") {
                        en = false;
                    } else {
                        en = true;
                    }
                    
                    return openarchive::success; 

                }
            } 
            
            return (std::error_code (EINVAL, std::generic_category()));
        }
        
        std::error_code gfapi_iopx::get_shard_size (file_ptr_t fp, 
                                                    uint64_t & shard_size)
        {
            shard_size = 0x400000;
            return openarchive::success;
         
        }
            
        std::error_code gfapi_iopx::gethosts (file_ptr_t fp, req_ptr_t req)
        {
            /*
             * We will parse the gluster volume information and get the 
             * list of hosts which are currently part of the volume.
             */ 

            std::string vol = fp->get_loc ().get_store ();
            std::string vol_file = std::string("/var/lib/glusterd/vols/") +
                                   volume + "/info";

            std::set<std::string> hosts;
            std::string str;
            std::ifstream inp;
            inp.open (vol_file.c_str ());

            while(getline (inp, str)) {
                if (str.substr (0,6) == std::string("brick-")) {
                    /*
                     * We are now left with a string in the following format.
                     * brick-0=gluster1:-mnt-ssd-brick
                     * From this we need to extract the server name gluster1
                     */ 
                    size_t start = str.find_first_of ("=");
                    if (start != std::string::npos) {
                        size_t end = str.find_first_of (":", start+1);
                        if (end != std::string::npos) {
                            /*
                             * Now that we have the start and end positions,
                             * we will form a sub string
                             */
                            end--;
                            std::string hname = str.substr (start+1, end-start);
                            hosts.insert (hname);
                        }
                    }     
                } else {
                    continue;
                }
            }
        
            inp.close ();   

            std::list<std::string> *lptr = req->get_phosts ();
            if (lptr) {
                lptr->clear ();
                std::set<std::string>::iterator iter;
                for(iter = hosts.begin (); iter != hosts.end(); iter++) {
                    lptr->push_back (boost::trim_all_copy(*iter)); 
                }     
            } 

            return openarchive::success; 

        }

        std::error_code gfapi_iopx::dup (file_ptr_t src_fp, file_ptr_t dest_fp)
        {
            glfs_fd_t *dest_glfd = NULL, *src_glfd = NULL;

            if (!ready) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glfs instance is not ready";
                return (std::error_code (EIO, std::generic_category()));
            }
 
            if (!fptrs.gl_dup) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glfs_dup entry point";
                return (std::error_code (ENOSYS, std::generic_category()));
            }

            file_info_t info;
            if (!(src_fp->get_file_info (get_name(), info))) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find glusterfs fd info "
                               << "for " << get_name() << "iopx for file "
                               << src_fp->get_loc().get_pathstr();

                return (std::error_code (ENOENT, std::generic_category()));
            }

            src_glfd = info.get_glfd (); 
            if (!src_glfd) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " glusterfs fd invalid  for " << get_name()
                               << "iopx for file "
                               << src_fp->get_loc().get_pathstr();

                return (std::error_code (ENXIO, std::generic_category()));
            }


            dest_glfd = fptrs.gl_dup (src_glfd);
            if (!dest_glfd) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to dup file handle for: "
                               << dest_fp->get_loc().get_pathstr()
                               << " error: " << strerror(errno);  

                return (std::error_code (errno, std::generic_category()));
            }

            /*
             * The file has been duped successfully. Increase the ref count 
             * for glfs_t instance object. 
             */ 
            get();

            /*
             * Save the fd for future fops.
             */ 
            info.set_glfd (dest_glfd); 
            dest_fp->set_file_info (get_name(), info);

            if (log_level >= openarchive::logger::level_debug_2) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                               << " Successfully duped file: "
                               << dest_fp->get_loc().get_pathstr();
            }

            return openarchive::success;
        }   

        std::error_code gfapi_iopx::sessionexists (std::string &vol)
        {
            
            std::string cmd = GLUSTERFIND;
            cmd += std::string (" ");
            cmd += std::string ("list --session");
            cmd += std::string (" ");
            cmd += std::string (SESSION_NAME);
            cmd += std::string (" ");
            cmd += std::string ("--volume");
            cmd += std::string (" ");
            cmd += vol;
            cmd += std::string (" > /dev/null 2>&1"); 
           
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " will execute command: " << cmd;
            }

            int exit_code = ::system (cmd.c_str()); 
            if (exit_code) {
                return (std::error_code (ENOENT, std::generic_category()));
            }

            return openarchive::success;
        }

        std::error_code gfapi_iopx::makesession (std::string &vol)
        {
            std::string cmd = GLUSTERFIND;
            cmd += std::string (" ");
            cmd += std::string ("create");
            cmd += std::string (" ");
            cmd += std::string (SESSION_NAME);
            cmd += std::string (" ");
            cmd += vol;
            cmd += std::string (" > /dev/null 2>&1"); 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " will execute command: " << cmd;
            }

            int exit_code = ::system (cmd.c_str()); 
            if (exit_code) {
                return (std::error_code (EPERM, std::generic_category()));
            }

            return openarchive::success;
        }  

        std::error_code gfapi_iopx::start_fullscan (std::string &vol, 
                                                    std::string &path)
        {
            std::string cmd = GLUSTERFIND;
            cmd += std::string (" pre ");
            cmd += std::string (SESSION_NAME);
            cmd += std::string (" ");
            cmd += vol;
            cmd += std::string (" ");
            cmd += path;
            cmd += std::string (" --full --regenerate-outfile --no-encode");
            cmd += std::string (" > /dev/null 2>&1"); 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " will execute command: " << cmd;
            }

            int exit_code = ::system (cmd.c_str()); 
            if (exit_code) {
                return (std::error_code (EACCES, std::generic_category()));
            }

            return openarchive::success;
        }    

        std::error_code gfapi_iopx::start_incrementalscan (std::string &vol,
                                                           std::string &path)
        {
            std::string cmd = GLUSTERFIND;
            cmd += std::string (" pre ");
            cmd += std::string (SESSION_NAME);
            cmd += std::string (" ");
            cmd += vol;
            cmd += std::string (" ");
            cmd += path;
            cmd += std::string (" --regenerate-outfile --no-encode");
            cmd += std::string (" > /dev/null 2>&1"); 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " will execute command: " << cmd;
            }

            int exit_code = ::system (cmd.c_str()); 
            if (exit_code) {
                return (std::error_code (EACCES, std::generic_category()));
            }

            return openarchive::success;
        } 

        std::error_code gfapi_iopx::scan (file_ptr_t fp, req_ptr_t req)
        {
            /*
             * Get the store name. Store name is supposed to be the volume name.
             */
            arch_loc_t loc = fp->get_loc ();
            std::string vol = loc.get_store ();
            std::string *path = req->get_str ();

            if (!path) {
                /*
                 * Invalid pointer
                 */
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " invalid pointer while scanning volume "
                               << vol;
                return (std::error_code (EFAULT, std::generic_category()));
            }

            std::string lock_file; 
            glfs_fd_t * fd = mklockfile (vol, lock_file);
            if (!fd) {
                /*
                 * Invalid file descriptor 
                 */
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " Failed to create lock file another backup"
                               << " may be active for the same session file"
                               << " for volume " << vol;
                return (std::error_code (EBUSY, std::generic_category()));
            }

            std::error_code ec = sessionexists (vol);
            if (ec != ok) {
                /*
                 * Session does not appear to be present. Create one.
                 */ 
                ec = makesession (vol);
                if (ec != ok) {

                    rmlockfile (fd, lock_file);
                    /*
                     * Failed to allocate the session
                     */  
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " failed to allocate session for"
                                   << " backup on volume " <<vol;

                    return (std::error_code (ENOENT, std::generic_category()));
                }
            }

            archstore_scan_type_t type = (archstore_scan_type_t)(req->get_flags ());
            std::string tmpfile;
            std::string arg;

            /*
             * Based on the type of scan determine the files to be backed up.
             */
            if (type == FULL) {

                arg = "full";
                mktempname (vol, arg, path, tmpfile);
                ec = start_fullscan (vol, tmpfile);

            } else {

                arg = "incr";
                mktempname (vol, arg, path, tmpfile);
                ec = start_incrementalscan (vol, tmpfile);
            }     

            /*
             * Create the exclude set. The files in this set will not be 
             * added to the collect file.
             */
            std::set <std::string> exclude_set;
            exclude_set.insert (lock_file);
 
            /*
             * We will now have to process the generated the entries and from it
             * create the collect file
             */   
            std::string genfile;
            std::string genarg ("gen");
            mktempname (vol, genarg, path, genfile);

            size_t entries = 0;
            ec = mkcollectfile (tmpfile, exclude_set, genfile, entries);

            if (ec == ok) {

                std::string collectfile;
                genarg = "iopx";
                mkcollectfilename (vol, arg, genarg, entries, path, 
                                   collectfile);

                /*
                 * Rename the generated collect file.
                 */
                int rc = ::rename(genfile.c_str (), collectfile.c_str ());
                if (rc) {
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " failed to rename the generated file to "
                                   << collectfile;

                    ec = std::error_code (errno, std::generic_category());  
                }

                ec = savefilename (path, collectfile); 
            } 

            rmlockfile (fd, lock_file);

            if (ec != ok) {
                /*
                 * Scan failed
                 */  
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to scan files for backup on volume "
                               << vol;
                return ec;
            }
  
            return openarchive::success;
        }

        glfs_fd_t * gfapi_iopx::mklockfile (std::string &vol, 
                                            std::string &file_path)
        {
            
            if (!fptrs.gl_creat) {
                return NULL;
            }

            std::string session (SESSION_NAME);

            mklockfilepath (vol, session, file_path);

            glfs_fd_t * glfd = fptrs.gl_creat (glfs, 
                                               file_path.c_str(),
                                               O_EXCL|O_CREAT, 0640);

            if (!glfd) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to create lock file " << file_path
                               << " on volume " << vol;
            }

            return glfd;
        }
            
        void gfapi_iopx::mklockfilepath (std::string &vol, std::string &session,
                                         std::string &file_path)
        {
            file_path = vol + std::string (".lock.") + session;  
            return;
        }

        void gfapi_iopx::rmlockfile (glfs_fd_t * fd, std::string &file_path)
        {
            if (fptrs.gl_close && fd) {  
                fptrs.gl_close (fd);
            }

            if (fptrs.gl_unlink) {
                fptrs.gl_unlink (glfs, file_path.c_str());
            }

            return;
        }

        void gfapi_iopx::mktempname (std::string &vol, std::string &arg,
                                     std::string *file_path, std::string &name)
        {
            std::string session (SESSION_NAME);
 
            char *temp = strdupa ((*file_path).c_str());
            name = dirname (temp);
            name += "/" + vol + "-" + session + "-" + arg;

            return;
        } 

        std::error_code gfapi_iopx::mkcollectfile (std::string &inp,
                                                   std::set <std::string> &excl,
                                                   std::string &outp,
                                                   size_t &entries)
        {
            std::ifstream inpstream;
            inpstream.open (inp);

            if (!inpstream.is_open ()) {
                /*
                 * Failed to open the file containing list of items to be
                 * processed.
                 */  
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to open " << inp;
                std::error_code ec (errno, std::generic_category());
                return (ec); 
            }

            std::ofstream outpstream;
            outpstream.open (outp);

            if (!outpstream.is_open ()) {
                /*
                 * Failed to open the output file 
                 */  
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to open " << outp;
                std::error_code ec (errno, std::generic_category());
                return (ec); 
            }

            entries = 0;
            while(inpstream.good ()) {
                std::string type (""),entry ("");
                inpstream >>type >>entry;

                if (type.length() && entry.length()) {
                    if (type == "NEW" || type == "MODIFY") {
                        if (excl.find (entry) == excl.end ()) {
                            outpstream << entry << std::endl;   
                            entries++;   
                        }
                    }
                }
            } 

            return openarchive::success;
        } 

        void gfapi_iopx::mkcollectfilename (std::string &vol, std::string &arg,
                                            std::string &desc, size_t entries,
                                            std::string *file_path, 
                                            std::string &name)
        {
            std::string session (SESSION_NAME);
 
            char *tmp = strdupa ((*file_path).c_str());
            name = dirname (tmp);
            name += "/" + vol + "-" + session + "-" + desc +
                    "." + boost::lexical_cast<std::string>(entries);

            return;
        } 
        
        std::error_code gfapi_iopx::savefilename (std::string *path, 
                                                  std::string &collectfile)
        {
            std::ofstream outpstream;
            outpstream.open (*path);

            if (outpstream.is_open ()) {
                outpstream << collectfile << std::endl;   
                return openarchive::success;
            }

            std::error_code ec (errno, std::generic_category());
            return ec;
        }  

    }
}
