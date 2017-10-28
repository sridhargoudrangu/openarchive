/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <arch_core.h>
#include <vfs_intfx.h>
#include <arch_loc.h>

namespace openarchive
{
    namespace vfs_intfx
    {
        vfs_intfx::vfs_intfx (void): mode(0), fd(-1)
        {
        }

        vfs_intfx::~vfs_intfx (void)
        {
            close ();
        }

        std::error_code vfs_intfx::open (loc_ptr_t loc, uint32_t flags)
        {
            path = loc;
            mode = flags;

            if (fd >= 0) {
                close ();
            }

            fd = ::open (path->get_pathstr().c_str(), flags);
            if (fd < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to open  " <<path->get_pathstr ()
                               << " flags: " <<flags; 

                std::error_code ec (errno, std::generic_category ());
                return (ec);
            }

            return (openarchive::success);
          
        }

        std::error_code vfs_intfx::close (void)
        {
            if (fd >= 0) {
                ::close (fd);
            }

            return (openarchive::success);
        }

        std::error_code vfs_intfx::read (struct iovec & splice,
                                         int64_t &bytes_read)
        {
            if (fd < 0) {
                std::error_code ec (EPERM, std::generic_category ());
                return (ec);
            }

            uint64_t count = splice.iov_len;
            bytes_read = 0; 
           
            while(count) {
                int64_t ret = ::read (fd, (char *)splice.iov_base+bytes_read, 
                                      splice.iov_len-bytes_read);

                if (ret < 0) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " read failed for " 
                                   << path->get_pathstr () << " : " 
                                   << path->get_uuidstr ()
                                   << " error code: " << errno 
                                   << " error desc: " << strerror (errno);

                    std::error_code ec (errno, std::generic_category ());
                    return (ec);
                }

                count -= ret;
                bytes_read += ret;
            }
            
            return (openarchive::success);
        }

        std::error_code vfs_intfx::pread (uint64_t offset, 
                                          struct iovec & splice,
                                          int64_t &bytes_read)
        {
            if (fd < 0) {
                std::error_code ec (EPERM, std::generic_category ());
                return (ec);
            }

            uint64_t count = splice.iov_len;
            bytes_read = 0; 

            while(count) {
                int64_t ret = ::pread (fd, (char *)splice.iov_base+bytes_read, 
                                       splice.iov_len-bytes_read, 
                                       offset+bytes_read);
            
                if (ret < 0) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " pread failed for " 
                                   << path->get_pathstr () << " : " 
                                   << path->get_uuidstr ()
                                   << " error code: " << errno 
                                   << " error desc: " << strerror (errno);

                    std::error_code ec (errno, std::generic_category ());
                    return (ec);
                }

                count -= ret;
                bytes_read += ret;
            }
            
            return (openarchive::success);
        }

        std::error_code vfs_intfx::write( struct iovec & splice, 
                                          int64_t &bytes_written)
        {
            if (fd < 0) {
                std::error_code ec (EPERM, std::generic_category ());
                return (ec);
            }

            uint64_t count = splice.iov_len;
            bytes_written = 0; 

            while(count) {
                int64_t ret = ::write(fd, (char *)splice.iov_base+bytes_written,
                                      splice.iov_len-bytes_written);

                if ( ret< 0) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " write failed for " 
                                   << path->get_pathstr () << " : " 
                                   << path->get_uuidstr ()
                                   << " error code: " << errno 
                                   << " error desc: " << strerror (errno);

                    std::error_code ec (errno, std::generic_category ());
                    return (ec);
                }

                count -= ret;
                bytes_written += ret;
            }
            
            return (openarchive::success);
        }
        
        std::error_code vfs_intfx::pwrite( uint64_t offset, 
                                           struct iovec & splice, 
                                           int64_t &bytes_written)
        {
            if (fd < 0) {
                std::error_code ec (EPERM, std::generic_category ());
                return (ec);
            }

            uint64_t count = splice.iov_len;
            bytes_written = 0; 

            while(count) {
                int64_t ret = ::pwrite(fd, (char *)splice.iov_base+bytes_written,
                                       splice.iov_len-bytes_written,
                                       offset+bytes_written);
            
                if (ret < 0) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " pwrite failed for " 
                                   << path->get_pathstr () << " : " 
                                   << path->get_uuidstr ()
                                   << " error code: " << errno 
                                   << " error desc: " << strerror (errno);

                    std::error_code ec (errno, std::generic_category ());
                    return (ec);
                }

                count -= ret;
                bytes_written += ret;
            }

            return (openarchive::success);
        }
            
        std::error_code vfs_intfx::fstat (struct stat *pstat)
        {
            if (fd < 0) {
                std::error_code ec (EPERM, std::generic_category ());
                return (ec);
            }

            if (::fstat (fd, pstat)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " fstat failed for " 
                               << path->get_pathstr () << " : " 
                               << path->get_uuidstr ()
                               << " error code: " << errno 
                               << " error desc: " << strerror (errno);

                std::error_code ec (errno, std::generic_category ());
                return (ec);
            }

            return (openarchive::success);
        }

        std::error_code vfs_intfx::stat (loc_ptr_t loc, struct stat *pstat)
        {
            if (::stat (loc->get_pathstr().c_str(), pstat)) {
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " stat failed for " 
                               << loc->get_pathstr () << " : " 
                               << loc->get_uuidstr ()
                               << " error code: " << errno 
                               << " error desc: " << strerror (errno);

                std::error_code ec (errno, std::generic_category ());
                return (ec);
            }

            return (openarchive::success);
        }

        std::error_code vfs_intfx::ftruncate (uint64_t size) 
        {
            if (fd < 0) {
                std::error_code ec (EPERM, std::generic_category ());
                return (ec);
            }

            if (::ftruncate (fd, size)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " ftruncate failed for " 
                               << path->get_pathstr () << " : " 
                               << path->get_uuidstr ()
                               << " error code: " << errno 
                               << " error desc: " << strerror (errno);

                std::error_code ec (errno, std::generic_category ());
                return (ec);
            }

            return (openarchive::success);
        }
       
        std::error_code vfs_intfx::truncate (loc_ptr_t loc, uint64_t size)
        {
            if (::truncate (loc->get_pathstr().c_str(), size)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " truncate failed for " 
                               << loc->get_pathstr () << " : " 
                               << loc->get_uuidstr ()
                               << " error code: " << errno 
                               << " error desc: " << strerror (errno);

                std::error_code ec (errno, std::generic_category ());
                return (ec);
            }

            return (openarchive::success);
        }

        std::error_code vfs_intfx::setxattr (loc_ptr_t loc, std::string name, 
                                             void *value, uint64_t len, 
                                             int32_t flags)
        {
            if (::setxattr (loc->get_pathstr().c_str(), name.c_str(), value, 
                            len, flags)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " setxattr failed for " 
                               << loc->get_pathstr () << " : " 
                               << loc->get_uuidstr ()
                               << " error code: " << errno 
                               << " error desc: " << strerror (errno);

                std::error_code ec (errno, std::generic_category ());
                return (ec);
            }

            return (openarchive::success);
        }

        std::error_code vfs_intfx::fsetxattr (std::string name, void *value, 
                                              uint64_t len, int32_t flags)
        {
            if (::fsetxattr (fd, name.c_str(), value, len, flags)) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " fsetxattr failed for " 
                               << path->get_pathstr () << " : " 
                               << path->get_uuidstr ()
                               << " error code: " << errno 
                               << " error desc: " << strerror (errno);

                std::error_code ec (errno, std::generic_category ());
                return (ec);
            }

            return (openarchive::success);
        }

        std::error_code vfs_intfx::getxattr (loc_ptr_t loc, std::string name, 
                                             void *value, uint64_t len, 
                                             int64_t &ret)
        {
            ret = ::getxattr(loc->get_pathstr().c_str(), name.c_str(), value,
                             len);

            if (ret < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " getxattr failed for " 
                               << loc->get_pathstr () << " : " 
                               << loc->get_uuidstr ()
                               << " error code: " << errno 
                               << " error desc: " << strerror (errno);

                std::error_code ec (errno, std::generic_category ());
                return (ec);
            }

            return (openarchive::success);
        }

        std::error_code vfs_intfx::fgetxattr (std::string name, void *value,
                                              uint64_t len, int64_t &ret)
        {
            ret = ::fgetxattr(fd, name.c_str(), value, len);

            if (ret < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " fgetxattr failed for " 
                               << path->get_pathstr () << " : " 
                               << path->get_uuidstr ()
                               << " error code: " << errno 
                               << " error desc: " << strerror (errno);

                std::error_code ec (errno, std::generic_category ());
                return (ec);
            }

            return (openarchive::success);
        }

        std::error_code vfs_intfx::removexattr (loc_ptr_t loc, std::string name)
        {
            if (::removexattr(loc->get_pathstr().c_str(), name.c_str()) < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " removexattr failed for " 
                               << loc->get_pathstr () << " : " 
                               << loc->get_uuidstr ()
                               << " error code: " << errno 
                               << " error desc: " << strerror (errno);

                std::error_code ec (errno, std::generic_category ());
                return (ec);
            }

            return (openarchive::success);
        }

        std::error_code vfs_intfx::fremovexattr (std::string name)
        {
            if (::fremovexattr(fd, name.c_str()) < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " removexattr failed for " 
                               << path->get_pathstr () << " : " 
                               << path->get_uuidstr ()
                               << " error code: " << errno 
                               << " error desc: " << strerror (errno);

                std::error_code ec (errno, std::generic_category ());
                return (ec);
            }

            return (openarchive::success);
        }

        std::error_code vfs_intfx::sendfile (boost::shared_ptr<vfs_intfx> out, 
                                             uint64_t offset, 
                                             uint64_t size, int64_t&ret)
        {
            off_t inp_offset = (off_t) offset;

            ret = ::sendfile64(out->fd, fd, &inp_offset, size); 
            if (ret< 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " sendfile64 failed to copy data from " 
                               << path->get_pathstr () << " : " 
                               << path->get_uuidstr () << " to "
                               << out->path->get_pathstr () << " : " 
                               << out->path->get_uuidstr () << " at offset: "
                               << offset 
                               << " error code: " << errno 
                               << " error desc: " << strerror (errno);

                std::error_code ec (errno, std::generic_category ());
                return (ec);
            }

            return (openarchive::success);
        }

        std::error_code vfs_intfx::receivefile (boost::shared_ptr<vfs_intfx> in,
                                                boost::shared_ptr<arch_pipe> pi,
                                                uint64_t in_offset,
                                                uint64_t out_offset,
                                                uint64_t size, 
                                                int64_t &rx_bytes)
        {

            if (pi->fds[0] < 0 || pi->fds[1] < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " receivefile from " 
                               << in->path->get_pathstr () << " : " 
                               << in->path->get_uuidstr () << " to "
                               << path->get_pathstr () << " : " 
                               << path->get_uuidstr () 
                               << " failed due to broken pipe ";

                std::error_code ec (EPIPE, std::generic_category ());
                return (ec);
            }

            loff_t inpoff = in_offset;
            loff_t outpoff = out_offset;

            ssize_t count = size; 
            rx_bytes = 0;

            do {
                ssize_t rxd = ::splice(in->fd, &inpoff,
                                       pi->fds[1], NULL, 
                                       count, 
                                       SPLICE_F_MOVE | SPLICE_F_MORE);

                if (rxd <= 0) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " splice failed for " 
                                   << in->path->get_pathstr ()
                                   << in->path->get_uuidstr ()
                                   << " error code: " << errno 
                                   << " error desc: " << strerror (errno);

                    std::error_code ec (errno, std::generic_category());
                    return(ec);
                }

                count -= rxd;

                do {
                    ssize_t  ret = ::splice(pi->fds[0], NULL, fd, 
                                            &outpoff, rxd,
                                            SPLICE_F_MOVE | SPLICE_F_MORE);

                    if (ret <= 0) {
                        BOOST_LOG_FUNCTION ();
                        BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                       << " splice failed for " 
                                       << path->get_pathstr ()
                                       << path->get_uuidstr ()
                                       << " error code: " << errno 
                                       << " error desc: " << strerror (errno);

                        std::error_code ec (errno, std::generic_category());
                        return(ec);
                    }

                    rxd -= ret;
                    rx_bytes += ret;

                } while (rxd);

            } while (count); 

            return (openarchive::success);
        }

    } /* namespace vfs_intfx */
} /* namespace openarchive */
