/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __META_IOPX_H__
#define __META_IOPX_H__

#include <arch_iopx.h>
#include <arch_tls.h>
#include <mem_cache.h>
#include <cfgparams.h>

namespace openarchive
{
    namespace meta_iopx
    {
        class meta_iopx: public openarchive::arch_iopx::arch_iopx
        {
            src::severity_logger<int> log; 
            uint32_t ttl;
            int32_t log_level; 

            private: 
            mem_cache_ptr_t get_mcache (file_ptr_t);
            std::string form_key (file_ptr_t, req_ptr_t);
            std::error_code pxsetxresp (file_ptr_t, req_ptr_t);
            std::error_code pxgetxreq (file_ptr_t, req_ptr_t, bool &);

            public:
            meta_iopx (std::string, io_service_ptr_t, uint32_t);
            ~meta_iopx (void);
            std::error_code fsetxattr    (file_ptr_t, req_ptr_t);
            std::error_code setxattr     (file_ptr_t, req_ptr_t);
            std::error_code fgetxattr    (file_ptr_t, req_ptr_t);
            std::error_code getxattr     (file_ptr_t, req_ptr_t);
            std::error_code fremovexattr (file_ptr_t, req_ptr_t);
            std::error_code removexattr  (file_ptr_t, req_ptr_t);
        };
    }

    typedef openarchive::meta_iopx::meta_iopx meta_iopx_t; 
    typedef boost::shared_ptr <meta_iopx_t> meta_iopx_ptr_t;

}

#endif
