/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __ARCH_TLS_H__
#define __ARCH_TLS_H__

#include <arch_file.h>
#include <iopx_reqpx.h>
#include <arch_mem.hpp>
#include <logger.h>
#include <arch_core.h>
#include <mem_cache.h>
#include <cvlt_iopx.h>

namespace openarchive
{
    namespace arch_tls
    {
        /*
         * All the commonly used objects will be allocated from memory pools 
         * inside arch_tls. We will use this design to avoid lock contention 
         * for commonly used objects.
         */ 
        class arch_tls
        {
            openarchive::arch_mem::mempool <file_t>       file_pool;
            openarchive::arch_mem::mempool <req_t>        req_pool;
            mem_cache_ptr_t mcache;
            file_attr_ptr_t fattr;
            src::severity_logger<int> log;
            cvlt_stream_ptr_t cvstream;

            public:
            boost::shared_ptr <file_t> alloc_arch_file (void)
            {
                boost::shared_ptr <file_t> fp = file_pool.make_shared ();
                return fp;
            }
  
            boost::shared_ptr <req_t>  alloc_iopx_req  (void)
            {
                boost::shared_ptr <req_t> req = req_pool.make_shared ();
                return req;
            }

            mem_cache_ptr_t get_mcache (void)
            {
                return mcache;
            } 

            mem_cache_ptr_t alloc_mcache (std::list<std::string> &servers)
            {
                mcache = boost::make_shared <mem_cache_t> (servers);
                return mcache;
            }   

            void set_stream_ptr (cvlt_stream_ptr_t ptr)
            {
                cvstream = ptr;
                return;
            }

            cvlt_stream_ptr_t get_stream_ptr (void)
            {
                return cvstream;
            } 

            file_attr_ptr_t get_fattr (void) 
            {
                if (fattr) {
                    return fattr;
                }

                fattr = boost::make_shared <file_attr_t> (); 
                return fattr;
            } 

            void log_statistics (void);
        };

        boost::thread_specific_ptr<arch_tls> &  get_arch_tls (void);

    } /* namespace arch_tls */

    typedef openarchive::arch_tls::arch_tls arch_tls_t;
    typedef boost::thread_specific_ptr<arch_tls_t> & tls_ref_t; 

} /* namespace openarchive */

#endif
