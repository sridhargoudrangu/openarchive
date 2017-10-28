/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __ARCH_FILE__
#define __ARCH_FILE__

#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <map>
#include <glfs.h>
#include <glfs-handles.h>
#include <boost/make_shared.hpp>
#include <boost/variant.hpp>
#include <arch_core.h>
#include <arch_mem.hpp>
#include <arch_store.h>
#include <file_attr.h>
#include <cvlt_types.h>

namespace openarchive
{
    /*
     * Forward declaration of arch_iopx
     */     
    namespace arch_iopx
    {
        class arch_iopx;
    };
 
    typedef openarchive::arch_loc::arch_loc arch_loc_t;

    namespace arch_file
    {
        /*
         * A file may be represented in different ways depending on the 
         * underlying storage/file system. A generic definition consisting
         * of all such possible representations is made below. To add support
         * for any new file system/storage technology add a definition below.
         */

        enum data_type
        {
            GLFS_FD_DATA   = 1,
            CBK_INFO_DATA  = 2,
            FILE_ATTR_DATA = 3,
            CACHE_SLOT_NUM = 4,
            CVLT_STREAM    = 5,
            DMSTATS_DATA   = 6       
        };

        class file_info
        {
            enum data_type type; 
            boost::variant <glfs_fd_t *, 
                            arch_store_cbk_info_ptr_t,
                            file_attr_ptr_t, uint32_t,
                            cvlt_stream_t *,
                            dmstats_ptr_t> val;
            
            public:
            void set_glfd (glfs_fd_t *fd)       
            {
                type = GLFS_FD_DATA;
                val  = fd;     
            }
            
            void set_cbk_info (arch_store_cbk_info_ptr_t info)
            {
                type = CBK_INFO_DATA;
                val  = info;
            }

            void set_file_attr (file_attr_ptr_t info)
            {
                type = FILE_ATTR_DATA;
                val = info;
            }

            void set_slot_num (uint32_t slot)
            {
                type = CACHE_SLOT_NUM;
                val = slot;  
            }

            void set_cvlt_stream (cvlt_stream_t *stream)
            {
                type = CVLT_STREAM;
                val = stream; 
            }

            void set_dmstats (dmstats_ptr_t ptr)
            {
                type = DMSTATS_DATA;
                val = ptr; 
            }

            glfs_fd_t * get_glfd (void) 
            {
                assert (type == GLFS_FD_DATA);
                return boost::get<glfs_fd_t *> (val);
            }

            arch_store_cbk_info_ptr_t get_cbk_info (void)
            {
                assert (type == CBK_INFO_DATA);
                return boost::get<arch_store_cbk_info_ptr_t> (val);
            }

            file_attr_ptr_t get_file_attr (void)
            {
                assert (type == FILE_ATTR_DATA);
                return boost::get<file_attr_ptr_t> (val);
            }

            uint32_t get_slot_num (void)
            {
                assert (type == CACHE_SLOT_NUM);
                return boost::get<uint32_t> (val);
            }

            cvlt_stream_t * get_cvlt_stream (void)
            {
                assert (type == CVLT_STREAM);
                return boost::get<cvlt_stream_t *> (val);
            } 
            
            dmstats_ptr_t get_dmstats (void)
            {
                assert (type == DMSTATS_DATA);
                return boost::get<dmstats_ptr_t> (val);
            }

        };

        class arch_file
        {
            arch_loc_t loc;
            volatile bool failed;
            std::atomic<bool> cbk_invoked; 
            time_t ref_time;
            openarchive::arch_core::spinlock lock;
            std::map<std::string, file_info> nvstore;
            boost::shared_ptr <openarchive::arch_iopx::arch_iopx> iopx;
            size_t file_size; 

            public:
            arch_file (void): failed (false), cbk_invoked(false)
            {
                nvstore.clear ();
            }

            arch_file (openarchive::arch_loc::arch_loc &);  
            ~arch_file (void);

            void          set_loc       (arch_loc_t & l)  { loc = l;           }
            void          set_loc       (arch_loc_ptr_t p){ loc = *p;          }
            void          set_failed    (bool b)          { failed = b;        }
            void          set_ref_time  (time_t t)        { ref_time = t;      }

            bool          set_cbk_invoked (void)          
            { 
                return cbk_invoked.exchange (true);
            }

            void set_iopx (boost::shared_ptr <openarchive::arch_iopx::arch_iopx> px) 
            { 
                iopx = px; 
                return;
            }

            void set_file_size (size_t sz)                { file_size = sz;    }

            arch_loc_t &  get_loc       (void)            { return loc;        }
            bool          get_failed    (void)            { return failed;     }
            time_t        get_ref_time  (void)            { return ref_time;   }

            boost::shared_ptr <openarchive::arch_iopx::arch_iopx> get_iopx (void)
            {
                return iopx;
            } 

            size_t get_file_size (void)                   { return file_size;  }

            void set_file_info   (std::string, file_info &);
            void erase_file_info (std::string);
            bool get_file_info   (std::string, file_info &);
        }; 

    } /* Namespace arch_file */


    typedef openarchive::arch_file::file_info        file_info_t;
    typedef boost::shared_ptr<file_info_t>           file_info_ptr_t;

    typedef openarchive::arch_file::arch_file        file_t;
    typedef boost::shared_ptr<file_t>                file_ptr_t;

} /* Namespace open_archive */

#endif /* End of __ARCH_FILE__ */
