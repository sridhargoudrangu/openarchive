/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <meta_iopx.h>

namespace openarchive
{
    namespace meta_iopx
    {
        meta_iopx::meta_iopx (std::string name, io_service_ptr_t svc,
                              uint32_t time): 
                              openarchive::arch_iopx::arch_iopx (name, svc),
                              ttl (time)
        {
            log_level = openarchive::cfgparams::get_log_level();
        } 

        meta_iopx::~meta_iopx (void)
        {
        }

        mem_cache_ptr_t meta_iopx::get_mcache (file_ptr_t fp)
        {
            tls_ref_t tls_ref = openarchive::arch_tls::get_arch_tls ();

            mem_cache_ptr_t mcache = tls_ref->get_mcache ();
            if (!mcache) {
                /*
                 * mem_cache interface is not yet set up for this thread.
                 * allocate one. Get the list of memcached servers.
                 */
                req_ptr_t req = tls_ref->alloc_iopx_req ();
                std::list <std::string> host_list;
                init_gethosts_req (fp, req, &host_list);

                std::error_code ec = get_first_child ()->gethosts (fp, req);
                if (ec == ok) {
                    /*
                     * Found hosts that contain the given file.
                     */ 
                    mcache = tls_ref->alloc_mcache (host_list);
                }else { 
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " failed to hosts for file " 
                                   << fp->get_loc().get_pathstr()
                                   << " error code: " << errno
                                   << " error desc: " << strerror(errno);
                }


            }

            return mcache; 
        }

        std::string meta_iopx::form_key (file_ptr_t fp, req_ptr_t req)
        {
            std::string key = fp->get_loc ().get_uuidstr () +
                              std::string (".") + req->get_desc ();  

            return key;
        } 

        std::error_code meta_iopx::pxsetxresp (file_ptr_t fp, req_ptr_t req)
        {
            mem_cache_ptr_t mcache = get_mcache (fp);
            void *buff = openarchive::iopx_req::get_xtattr_baseaddr (req);
            size_t size = req->get_len();

            kvpair_t kv; 
            kv.key = form_key (fp, req);
            kv.value.iov_base = buff;
            kv.value.iov_len = size;
            kv.ttl = ttl;
 
            std::error_code ec = mcache->set (kv);
            return ec;
        }
    
        std::error_code meta_iopx::fsetxattr (file_ptr_t fp, req_ptr_t req)
        {
            /*
             * We will invoke the child iopx. If the call succeeds @ child 
             * iopx, then we will update the memcached with the extended 
             * attributes so that any future requests will be served out
             * of the cache.
             */

            std::error_code ec = get_first_child ()->fsetxattr (fp, req);

            if (ec == ok) {
                pxsetxresp (fp, req);
            }

            return ec;
        }

        std::error_code meta_iopx::setxattr (file_ptr_t fp, req_ptr_t req)
        {
            /*
             * We will invoke the child iopx. If the call succeeds @ child 
             * iopx, then we will update the memcached with the extended 
             * attributes so that any future requests will be served out
             * of the cache.
             */

            std::error_code ec = get_first_child ()->setxattr (fp, req);

            if (ec == ok) {
                pxsetxresp (fp, req);
            }

            return ec;
        }

        std::error_code meta_iopx::pxgetxreq (file_ptr_t fp, req_ptr_t req, 
                                              bool & found)
        {
            mem_cache_ptr_t mcache = get_mcache (fp);
            kvpair_t kv; 
            kv.key = form_key (fp, req);
            found = false;

            std::error_code ec = mcache->get (kv);
            if (ec == ok) {

                /*
                 * Found a valid entry in memcached
                 */
                if (log_level >= openarchive::logger::level_debug_2) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                                   << "Cache hit for " << kv.key; 
                }

                void *buff = openarchive::iopx_req::get_xtattr_baseaddr (req);
                if (buff && req->get_len() >= kv.value.iov_len) {
                    memcpy (buff, kv.value.iov_base, kv.value.iov_len);
                    req->set_ret (kv.value.iov_len);
                    found = true;  
                } else if (!buff) {
                    /*
                     * This could be the case where application is trying to
                     * get the size of extended attribute.
                     */  
                    req->set_ret (kv.value.iov_len);
                    found = true;  
                }


            } else {

                if (log_level >= openarchive::logger::level_debug_2) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                                   << "Cache miss for " << kv.key; 
                }
            }  
            
            return openarchive::success; 
        }

        std::error_code meta_iopx::fgetxattr (file_ptr_t fp, req_ptr_t req)
        {
            /*
             * We will check whether the memcached contains the extended 
             * attributes. If they are not present in memcached then we 
             * will invoke the child iopx.
             */ 
            bool found = false;
            std::error_code ec = pxgetxreq (fp, req, found);
            if (ec != ok || found) {
                return ec;
            }
  
            /*
             * No entry exists in memcached currently. Fetch the extended 
             * attribute and update memcached.
             */
            ec = get_first_child ()->fgetxattr (fp, req);
            if (ec != ok) {
                return ec;
            }
            
            mem_cache_ptr_t mcache = get_mcache (fp);
            void *buff = openarchive::iopx_req::get_xtattr_baseaddr (req);
            size_t size = req->get_ret ();

            if (buff) {

                /*
                 * In some cases fgetxattr can be invoked with NULL pointer to
                 * determine the length of extended attrribute.
                 */ 

                kvpair_t kv; 
                kv.key = form_key (fp, req);
                kv.value.iov_base = buff;
                kv.value.iov_len = size;
                kv.ttl = ttl;

                mcache->set (kv);

            }

            return openarchive::success; 
        }

        std::error_code meta_iopx::getxattr (file_ptr_t fp, req_ptr_t req)
        {
            /*
             * We will check whether the memcached contains the extended 
             * attributes. If they are not present in memcached then we 
             * will invoke the child iopx.
             */ 
            bool found = false;
            std::error_code ec = pxgetxreq (fp, req, found);
            if (ec != ok || found) {
                return ec;
            }
  
            /*
             * No entry exists in memcached currently. Fetch the extended 
             * attribute and update memcached.
             */
            ec = get_first_child ()->getxattr (fp, req);
            if (ec != ok) {
                return ec;
            }
            
            mem_cache_ptr_t mcache = get_mcache (fp);
            void *buff = openarchive::iopx_req::get_xtattr_baseaddr (req);
            size_t size = req->get_ret ();

            if (buff) {

                kvpair_t kv; 
                kv.key = form_key (fp, req);
                kv.value.iov_base = buff;
                kv.value.iov_len = size;
                kv.ttl = ttl;

                mcache->set (kv);
            }

            return openarchive::success; 
        }

        std::error_code meta_iopx::fremovexattr (file_ptr_t fp, req_ptr_t req)
        {
            /*
             * We will clear the entry from memcached. Then we will invoke
             * the child iopx.
             */
            kvpair_t kv; 
            kv.key = form_key (fp, req);

            mem_cache_ptr_t mcache = get_mcache (fp);
            std::error_code ec = mcache->drop (kv);
            if (ec != ok) {
                return ec;
            }

            return get_first_child ()->fremovexattr (fp, req);
        }

        std::error_code meta_iopx::removexattr (file_ptr_t fp, req_ptr_t req)
        {
            /*
             * We will clear the entry from memcached. Then we will invoke
             * the child iopx.
             */
            kvpair_t kv; 
            kv.key = form_key (fp, req);

            mem_cache_ptr_t mcache = get_mcache (fp);
            std::error_code ec = mcache->drop (kv);
            if (ec != ok) {
                return ec;
            }

            return get_first_child ()->removexattr (fp, req);
        }
    }
}
