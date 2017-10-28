/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __VFS_INTFX__
#define __VFS_INTFX__

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <attr/xattr.h>
#include <sys/sendfile.h>
#include <arch_loc.h>
#include <logger.h>
#include <arch_core.h>

namespace openarchive
{
    namespace vfs_intfx
    {
        class arch_pipe;
        
        class vfs_intfx
        {
            loc_ptr_t path;
            uint32_t mode; 
            int32_t fd;
            src::severity_logger<int> log; 

            public:
            vfs_intfx (void);
            ~vfs_intfx (void);

            std::error_code open (loc_ptr_t, uint32_t flags);
            std::error_code close (void);
           
            /*
             * Read variants
             */
            std::error_code read (struct iovec &, int64_t &);
            std::error_code pread (uint64_t, struct iovec &, int64_t &);

            /*
             * Write variants
             */
            std::error_code write (struct iovec &, int64_t &);
            std::error_code pwrite (uint64_t, struct iovec &, int64_t &);

            /*
             * Stat variants
             */
            std::error_code fstat (struct stat *);
            std::error_code stat (loc_ptr_t, struct stat*);

            /*
             * Truncate variants
             */
            std::error_code ftruncate (uint64_t); 
            std::error_code truncate (loc_ptr_t, uint64_t);

            /*
             * Extended attributes handling methods
             */ 
            std::error_code setxattr (loc_ptr_t, 
                                      std::string, void *, 
                                      uint64_t, int32_t);
            std::error_code fsetxattr (std::string, void *, uint64_t, int32_t);
            std::error_code getxattr (loc_ptr_t, std::string, 
                                      void *, uint64_t, int64_t &);
            std::error_code fgetxattr (std::string, void *, uint64_t, 
                                       int64_t &);
            std::error_code removexattr (loc_ptr_t, std::string);
            std::error_code fremovexattr (std::string);

            /*
             * Zero copy variants
             */ 
            std::error_code sendfile (boost::shared_ptr<vfs_intfx> , uint64_t, 
                                      uint64_t, int64_t&);
            std::error_code receivefile (boost::shared_ptr<vfs_intfx>, 
                                         boost::shared_ptr<arch_pipe>, uint64_t,
                                         uint64_t, uint64_t, int64_t &);

            /*
             * Seek variants
             */
            std::error_code lseek (uint64_t, int, int64_t &);

        };  

        class arch_pipe
        {
            int fds[2];
            src::severity_logger<int> log; 
            friend std::error_code vfs_intfx::receivefile (\
                                              boost::shared_ptr<vfs_intfx>,
                                              boost::shared_ptr<arch_pipe>,
                                              uint64_t, uint64_t, uint64_t,
                                              int64_t &);

            public:

            arch_pipe(void)
            {
                if (::pipe(fds) < 0) {
                    fds[0] = -1;
                    fds[1] = -1;
                    BOOST_LOG_SEV(log, openarchive::logger::level_error)\
                                  << " failed to allocate pipe";
                }
            }

            ~arch_pipe(void)
            {
                if (fds[0] >= 0) {
                    ::close(fds[0]);
                }

                if (fds[1] >= 0) {
                    ::close(fds[1]);
                }
            }
        
        };
    } /* namespace vfs_intfx */
} /* namespace openarchive */

#endif  /* End of __VFS_INTFX__ */

