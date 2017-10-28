/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <sys/types.h>
#include <sys/xattr.h>
#include <string>
#include <atomic>
#include <thread>
#include <chrono>
#include <queue>
#include <iterator>
#include <algorithm>
#include <thread>
#include <archivestore.h>
#include <arch_core.h>
#include <arch_iopx.h>
#include <iopx_reqpx.h>
#include <arch_engine.h>
#include <arch_mem.hpp>
#include <arch_store.h>
#include <arch_tls.h>
#include <file_attr.h>
#include <logger.h>

namespace openarchive
{
    namespace data_mgmt
    {
        const int num_cbk_info_alloc  = 32; 
        const int num_file_alloc      = 32;
        const int num_req_alloc       = 32;
        const int num_dmstat_alloc    = 32;
        const int num_file_attr_alloc = 32;
        const int num_loc_alloc       = 32;

        /*
         * data_mgmt is the interface to the external applications/products 
         * for performing any data management activity.
         * data_mgmt layer interacts with arch_engine layer to create an
         * iopx tree. Any data mgmt function boils down to creation of an
         * iopx tree and then invoking I/O on the created iopx tree.
         * There are two kinds of iopx trees that are maintained by the 
         * data_mgmt layer.
         * 1) Source iopx tree
         * 2) Sink iopx tree
         * For some of the data mgmt functions only a source tree is needed
         * while for some both source as well as sink trees are needed.
         *
         * Examples:
         *
         * READ:
         *      For reading from archive store only a source iopx tree
         *      is needed.
         * ARCHIVE:
         *      For archival data will be read from primary store using source 
         *      iopx and will be written to a archive store using sink iopx
         * RESTORE:
         *      For Restore data will be read from archive store using source
         *      iopx and will be  written to  primary store using sink iopx
         *     
         * Depending on the product and the datamanagement activities to be 
         * performed source/sink will be initialized with iopx tree.
         */



        class data_mgmt;
        typedef std::error_code (data_mgmt::* data_mgmt_worker) (arch_loc_t &, 
                                                                 arch_loc_t &,
                                                                 std::string, 
                                                                 dmstats_ptr_t,
                                                                 file_tracker_ptr_t, 
                                                                 arch_store_cbk_info_ptr_t);
        class data_mgmt
        {
            bool ready;
            uint64_t extent_size; /* Size of each extent */
            uint32_t num_bits; /* Extent size in bitwidth */
            iopx_ptr_t source; /* Tree of source iopx */
            iopx_ptr_t sink; /* Tree of sink iopx */
            arch_engine_ptr_t engine; /* Engine to be used for ops */
            /* 
             * ioservice for executing source iopx tree 
             */
            io_service_ptr_t src_iosvc; 
            /* 
             * ioservice for executing sink iopx tree 
             */
            io_service_ptr_t sink_iosvc; 
            src::severity_logger<int> log; 
            int32_t log_level;

            /*
             * Different object pools used for handling memory allocations 
             * and deallocations.
             */  
            openarchive::arch_mem::objpool <arch_store_cbk_info_t,
                                            num_cbk_info_alloc>     cbk_pool;
            openarchive::arch_mem::objpool <file_t, num_file_alloc> file_pool; 
            openarchive::arch_mem::objpool <req_t,  num_req_alloc>  req_pool; 
            openarchive::arch_mem::objpool <dmstats_t, 
                                            num_dmstat_alloc>       dmstat_pool;
            openarchive::arch_mem::objpool <file_attr_t,
                                            num_file_attr_alloc>    fileattr_pool;
            openarchive::arch_mem::objpool <arch_loc_t,
                                            num_loc_alloc>          loc_pool;

            /*
             * Spinlock for safegaurading allocations/deallocations of 
             * resources.
             */
            openarchive::arch_core::spinlock lock;

            std::thread * memprof_th; /* Memory profiler thread  */
            /*
             * Variable for keeping track of whether the memory profiler 
             * thread needs to be kept alive. Set it to false when profiler
             * threads is no longer needed.
             */
            volatile std::atomic<bool> done;

            private:
            void mem_prof (void);
            std::error_code run_cbk (file_ptr_t, int64_t, int32_t);
            void work_done_cbk (arch_store_cbk_info_ptr_t, dmstats_ptr_t,
                                int32_t, int32_t);
            void log_tls_stats (void);
            void log_tls_info (void);
            void map_store_id (std::string &, std::string &, std::string &);

            /*
             * Process the items stored in file
             * arg1 data management operation type
             * arg2 should fast ioservice be enabled
             * arg3 location of the store on which the items are currently
                    located
             * arg4 location of the store to which the items need to be moved
             * arg5 location of the store where failed items need to be located
             * arg6 worker to be invoked for processing each work item
             * arg7 callback handler to be invoked after all the items are 
                    processed
             */ 
            std::error_code process_items (arch_op_type, bool, arch_loc_t &,
                                           arch_loc_t &, arch_loc_t &,
                                           data_mgmt_worker, 
                                           arch_store_cbk_info_ptr_t);

            /*
             * arg1  location of the store where source files are located
             * arg2  location of the store where backed up data needs to be
             *       stored
             * arg3  name of the file on source store which contains list of
             *       files to be backed up by current worker  
             * arg4  statistics for house keeping
             * arg5  file pointer for saving failed files path
             * srg6  Callback handler
             */ 
               
            std::error_code backup_worker (arch_loc_t &, arch_loc_t &,
                                           std::string, dmstats_ptr_t,
                                           file_tracker_ptr_t, 
                                           arch_store_cbk_info_ptr_t);

            /*
             * Backup a file
             * arg1  location of the source from which data needs to be read
             * arg2  location of the destination to place backed up files
             * arg3  size of the file to be backed up
             * arg4  buffer for performing I/O operations
             * arg5  actual size of the file
             */ 
            std::error_code backup_file (arch_loc_t &, arch_loc_t &, size_t,
                                         buff_ptr_t, size_t);

            /*
             * arg1  location of the store where source files are located
             * arg2  location of the store where backed up data needs to be
             *       stored
             * arg3  name of the file on source store which contains list of
             *       files to be backed up by current worker  
             * arg4  statistics for house keeping
             * arg5  file pointer for saving failed files path
             * srg6  Callback handler
             */ 
            std::error_code archive_worker (arch_loc_t &, arch_loc_t &,
                                            std::string, dmstats_ptr_t,
                                            file_tracker_ptr_t,
                                            arch_store_cbk_info_ptr_t);

            /*
             * Backup a file
             * arg1  location of the file which needs to be archived
             */ 
            std::error_code archive_file (arch_loc_t &);

            /*
             * arg1  location of the file to be restored
             * arg2  destination location where the file will be restored
             * arg3  data management statistics pointer  
             * arg4  callback handler information
             */
            std::error_code restore_worker (arch_loc_t &, arch_loc_t &,
                                            dmstats_ptr_t,
                                            arch_store_cbk_info_ptr_t);

            /*
             * arg1  location of the file from which data needs to be read
             * arg2  offset with in the file
             * arg3  iovec for storing the data read
             * arg4  callback info
             */ 
            std::error_code read_splice (arch_loc_ptr_t, uint64_t,
                                         const struct iovec, 
                                         arch_store_cbk_info_ptr_t);

            /*
             * Send data from source store to destination store
             * arg1  source file descriptor
             * arg2  destination file descriptor
             * arg3  offset
             * arg4  buffer pointer
             * arg5  num of bytes to be sent
             * arg6  num of bytes actually sent
             */
              
            std::error_code sendfile (file_ptr_t, file_ptr_t, uint64_t,
                                      buff_ptr_t, uint64_t, uint64_t &);

            /*
             * Set the extended file system attributes which capture the 
             * metadata about a file before it is archived.
             * arg1  iopx for accessing the file
             * arg2  file descriptor
             * arg3  file attributes which need to be set
             * arg4  set backup extended attributes
             * arg5  set archive extended attributes
             */
            std::error_code fsetxattrs (iopx_ptr_t, file_ptr_t, file_attr_ptr_t,
                                        bool, bool);

            /*
             * Set an extended file system attribute
             * arg1  iopx for accessing the file
             * arg2  file descriptor
             * arg3  extended attribute name
             * arg4  extended attribute value
             * arg5  extended attribute size
             */ 
            std::error_code fsetxattr (iopx_ptr_t, file_ptr_t, std::string,
                                       void *, size_t );

            /*
             * Get size of the file from extended file system attribute
             * arg1  iopx for accessing the file
             * arg2  file descriptor
             * arg3  request descriptor
             * arg4  length of the file
             */ 
            std::error_code get_file_size (iopx_ptr_t, file_ptr_t, req_ptr_t,
                                           uint64_t &);
        
            /*
             * Truncate a file to given size 
             * arg1  iopx for accessing the file
             * arg2  file descriptor
             * arg3  request descriptor
             * arg4  length to which the file needs to be truncated
             */ 
            std::error_code ftruncate (iopx_ptr_t, file_ptr_t, req_ptr_t,
                                       uint64_t);

            /*
             * Allocate source iopx tree.
             */
            void alloc_src_iopx (iopx_tree_cfg_t &);

            /*
             * Allocate sink iopx tree.
             */
            void alloc_sink_iopx (iopx_tree_cfg_t &);
   
            public:
            data_mgmt (void);
            std::error_code set_extent_size (uint64_t);  
            void log_memory_stats (void);

            /*
             * Allocate a callback info object from the object pool. 
             * The object can be used for maintaining context in which 
             * an data management operation has been invoked and the 
             * context will be played back when the callback is invoked 
             * after the operation completes.
             */   
            arch_store_cbk_info_ptr_t make_shared_cbk_info (void)
            {
             
                arch_store_cbk_info_ptr_t ptr = cbk_pool.make_shared ();
                return ptr;
            }

            /*
             * Allocate a location object from location pool.
             */   
            arch_loc_ptr_t make_shared_loc (void) 
            {
                arch_loc_ptr_t ptr = loc_pool.make_shared ();
                return ptr;
            }

            virtual ~data_mgmt (void);

            /*
             * Find the changed files on a store
             * arg1  store from which the changed files need to be found.
             * arg2  type of scan whether full/incremental 
             * arg3  file path which contains list of files to be backed up
             */
            virtual std::error_code scan (arch_loc_t &, 
                                          archstore_scan_type_t,
                                          std::string &);
 
            /*
             * Backup list of items  
             * arg1  path containing list of files to be backed up
             * arg2  location of the destination to place backed up files
             * arg3  location containing failed files information 
             * arg4  callback handler information
             */
            virtual std::error_code backup_items (arch_loc_t &, arch_loc_t &,
                                                  arch_loc_t &,
                                                  arch_store_cbk_info_ptr_t);

            /*
             * Archive list of items  
             * arg1  path containing list of files to be archived 
             * arg2  location of the destination to place archived files
             * arg3  location containing failed files information 
             * arg4  callback handler information
             */
            virtual std::error_code archive_items (arch_loc_t &, arch_loc_t &,
                                                   arch_loc_t &,
                                                   arch_store_cbk_info_ptr_t);

            /*
             * Restore a file from archive store to any other store.
             * arg1  location of the file to be restored
             * arg1  destination location where the file will be restored
             * arg3  callback handler information
             */

            virtual std::error_code restore_file (arch_loc_t &, arch_loc_t &, 
                                                  arch_store_cbk_info_ptr_t);
            
            /*
             * Read the contents of a file from the specified location.
             * arg1  location of the file to be read
             * arg2  file offset 
             * arg3  iovec containing location of the buffer
             * arg4  callback handler information
             */  
            virtual std::error_code read_file (arch_loc_ptr_t, uint64_t,
                                               const struct iovec iov, 
                                               arch_store_cbk_info_ptr_t cbki);

        };
    } /* End of data_mgmt */

    typedef openarchive::data_mgmt::data_mgmt        data_mgmt_t;

} /* End of openarchive */
