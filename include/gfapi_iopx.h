/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __GFAPI_IOPX_H__
#define __GFAPI_IOPX_H__

#include <libgen.h>
#include <stdio.h>
#include <glfs.h>
#include <glfs-handles.h>
#include <boost/make_shared.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/algorithm/string/trim_all.hpp>
#include <boost/process.hpp>
#include <arch_iopx.h>
#include <iopx_reqpx.h>
#include <gfapi_fops.h>
#include <cfgparams.h>
#include <logger.h>

namespace openarchive
{
    namespace gfapi_iopx
    {
        class gfapi_iopx: public openarchive::arch_iopx::arch_iopx
        {
            std::string volume;
            std::string hname;
            std::string protocol;
            uint32_t port;
            openarchive::gfapi_fops::gfapi_fops fops; 
            struct openarchive::gfapi_fops::libgfapi_fops & fptrs;
            glfs_t *glfs; 
            bool ready;
            std::string gflogpath;
            uint32_t gfloglevel;
            src::severity_logger<int> log;
            int32_t log_level;

            private:
            std::error_code get_shard_size (file_ptr_t, uint64_t &);
            std::error_code extract_gfid (std::string, struct stat *,
                                          void *, uint32_t);
            std::error_code sharding_enabled (file_ptr_t, bool &);
            std::error_code sessionexists (std::string &);
            std::error_code makesession (std::string &);
            std::error_code start_fullscan (std::string &, std::string &);
            std::error_code start_incrementalscan (std::string &, std::string &);
            glfs_fd_t * mklockfile (std::string &, std::string &);
            void mklockfilepath (std::string &, std::string &, std::string &);
            void rmlockfile (glfs_fd_t *, std::string &);
            void mktempname (std::string &, std::string &, std::string *,
                             std::string &);
            std::error_code mkcollectfile (std::string &, 
                                           std::set <std::string> &excl, 
                                           std::string &, size_t &);
            void mkcollectfilename (std::string &, std::string &, std::string &,
                                    size_t, std::string*, std::string &);
            std::error_code savefilename (std::string *, std::string &);

            public:
            gfapi_iopx (std::string, io_service_ptr_t, std::string);
            ~gfapi_iopx (void);

            /*
             * File operations
             */
            virtual std::error_code open              (file_ptr_t, req_ptr_t);

            virtual std::error_code close             (file_ptr_t, req_ptr_t);

            virtual std::error_code close             (file_t &);

            virtual std::error_code pread             (file_ptr_t, req_ptr_t);

            virtual std::error_code pwrite            (file_ptr_t, req_ptr_t);

            virtual std::error_code fstat             (file_ptr_t, req_ptr_t);

            virtual std::error_code stat              (file_ptr_t, req_ptr_t);

            virtual std::error_code ftruncate         (file_ptr_t, req_ptr_t);

            virtual std::error_code truncate          (file_ptr_t, req_ptr_t);

            virtual std::error_code fsetxattr         (file_ptr_t, req_ptr_t);

            virtual std::error_code setxattr          (file_ptr_t, req_ptr_t);

            virtual std::error_code fgetxattr         (file_ptr_t, req_ptr_t);

            virtual std::error_code getxattr          (file_ptr_t, req_ptr_t);

            virtual std::error_code fremovexattr      (file_ptr_t, req_ptr_t);

            virtual std::error_code removexattr       (file_ptr_t, req_ptr_t);

            virtual std::error_code lseek             (file_ptr_t, req_ptr_t);

            virtual std::error_code getuuid           (file_ptr_t, req_ptr_t);

            virtual std::error_code gethosts          (file_ptr_t, req_ptr_t);

            /*
             * File system operations
             */
            virtual std::error_code mkdir             (file_ptr_t, req_ptr_t);

            virtual std::error_code resolve           (file_ptr_t, req_ptr_t);

            virtual std::error_code dup               (file_ptr_t, file_ptr_t);

            virtual std::error_code scan              (file_ptr_t, req_ptr_t);

        };
    }

    typedef openarchive::gfapi_iopx::gfapi_iopx gfapi_iopx_t;
    typedef boost::shared_ptr <gfapi_iopx_t> gfapi_iopx_ptr_t;

}
 
#endif
