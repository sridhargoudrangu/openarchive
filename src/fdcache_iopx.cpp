/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <fdcache_iopx.h>

namespace openarchive
{
    namespace fdcache_iopx
    {
        void fdcache_iopx::init_ra_buf (struct ra_buf &rabuf)
        {
            rabuf.valid = false;
            rabuf.busy = false; 
            rabuf.pending = false;
            rabuf.rd_in_progress = false;
            rabuf.offset = 0;
            rabuf.bytes = 0;

            return;
        }

        void fdcache_iopx::init_vec_entry (struct vec_entry &vec)
        {
            vec.valid = false;
            vec.busy = false;
            vec.pending = false;
            init_ra_buf (vec.rabuff);

            return;
        }

        void fdcache_iopx::reserve_vec_entry (struct vec_entry &vec)
        {
            vec.valid = false;
            vec.busy = true;
            vec.pending = true;
            init_ra_buf (vec.rabuff);

            return;
        }

        void fdcache_iopx::mark_vec_entry_ready (struct vec_entry &vec)
        {
            vec.valid = true;
            vec.busy = false;
            vec.pending = false;

            return;
        }

        void fdcache_iopx::reserve_ra_buf (struct ra_buf &rabuf)
        {
            rabuf.valid = false;
            rabuf.busy = true; 
            rabuf.pending = true;

            return;
        }

        void fdcache_iopx::mark_ra_buf_ready (struct ra_buf &rabuf)
        {
            rabuf.valid = true;
            rabuf.busy = false; 
            rabuf.pending = false;
            rabuf.rd_in_progress = false;

            return;
        }

        fdcache_iopx::fdcache_iopx (std::string name, io_service_ptr_t svc,
                                    uint32_t num):
                                    openarchive::arch_iopx::arch_iopx (name, svc),
                                    capacity(num+1),
                                    file_pool ("filepool"),
                                    req_pool ("reqpool"),
                                    buff_pool ("rabuffpool")
        {

            log_level = openarchive::cfgparams::get_log_level();
            /*
             * Reserve memory in the vector. 
             */
            fd_queue.reserve (capacity);

            for(uint32_t count = 0; count < capacity; count++) {
                struct vec_entry ventry;
                init_vec_entry (ventry);
                ventry.op_mtx = boost::make_shared<std::mutex> ();
                ventry.rabuff.rd_mtx = boost::make_shared<std::mutex> ();
                fd_queue.push_back (ventry);                
            }

            front = rear = 0;
        }

        fdcache_iopx::~fdcache_iopx (void)
        {
            req_ptr_t req = req_pool.make_shared ();

            wrlock_guard_t guard (&fdlock);

            for(uint32_t slot = 0; slot < capacity; slot++) {

                if (fd_queue[slot].valid) {

                    if (fd_queue[slot].fp) { 
                        get_first_child()->close (fd_queue[slot].fp, req);
                    }
 
                }
            }
        } 

        std::error_code fdcache_iopx::search_fd (file_ptr_t fp)
        {
            std::string uid = fp->get_loc ().get_uuidstr ();
            std::map <std::string, map_entry>::iterator it;
            file_ptr_t cache_fp;
            uint32_t slot;

            { 
                rdlock_guard_t guard (&fdlock);

                it = uuid_map.find (uid);

                if (it != uuid_map.end ()) {
                    if (it->second.valid) {
                        slot =  it->second.index;
                        if (fd_queue[slot].valid) {

                            /*
                             * We found a valid fd to the same file. We will 
                             * duplicate the fd reference.
                             */ 
                     
                            cache_fp = fd_queue[slot].fp;
                        }
                    }
                } 
            
                if (cache_fp) {

                    if (log_level >= openarchive::logger::level_debug_2) {
                        BOOST_LOG_FUNCTION ();
                        BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                                       << " found valid fd-cache entry for "
                                       << fp->get_loc().get_pathstr()
                                       << " : " << uid << " @ slot " << slot;
                    }
      
                    std::error_code ec = get_first_child ()->dup (cache_fp, fp);
                    if (ec == ok) {
                        /*
                         * Set slot number to be used for future operations
                         */  
                        file_info_t info;
                        info.set_slot_num (slot);
                        fp->set_file_info (get_name (), info);
                    }

                    return ec;
                }
            }

            return std::error_code (ENOENT, std::generic_category());

        }

        std::error_code fdcache_iopx::alloc_slot (file_ptr_t fp,
                                                  bool lock_rabuf, 
                                                  uint32_t & free_slot,
                                                  uint32_t & close_slot,
                                                  bool & needs_close)
        {
            std::string uid = fp->get_loc ().get_uuidstr ();
            std::map <std::string, map_entry>::iterator it;

            wrlock_guard_t guard (&fdlock);

            it = uuid_map.find (uid);
            needs_close = false;

            if (it != uuid_map.end ()) {
                /*
                 * An entry with this uuid already exists in the map
                 */   
                free_slot = it->second.index;
                if (!fd_queue[free_slot].valid && !fd_queue[free_slot].busy) { 
                    reserve_vec_entry (fd_queue[free_slot]);
                }
                return openarchive::success;
            }

            /*
             * No entry exists in the map currently. We will make the 
             * necessary allocations and get out of this funtion. File
             * specific opens/initializations will be done in other functions.
             * We will first allocate an entry in the circular buffer and then
             * update the entry in map.  
             */
            
            if ((front+1) % capacity ==  rear) {

                /*
                 * Circular buffer is full. Move the rear forward to 
                 * make space for new entry.
                 */

                if (fd_queue[rear].busy || fd_queue[rear].rabuff.busy) {
                    /*
                     * This is a busy slot.
                     */
                    return std::error_code (EADDRINUSE, std::generic_category());
                }

                if (fd_queue[rear].valid) {

                    /*
                     * We are about to remove the entry from circular buffer.
                     * It is not necessary to invoke the close here. Close will
                     * be invoked before allocating a new file descriptor.
                     */

                    std::string id = fd_queue[rear].fp->get_loc ().get_uuidstr ();
                    it = uuid_map.find (id);
                    if (it != uuid_map.end ()) {
                        /*
                         * Remove the index from map
                         */
                        uuid_map.erase (it);
                    } 
                  
                    init_vec_entry (fd_queue[rear]); 
                    close_slot = rear; 
                    needs_close = true;
                }

                rear=(rear+1) % capacity;

            }

            reserve_vec_entry (fd_queue[front]);
            if (lock_rabuf) {
                reserve_ra_buf (fd_queue[front].rabuff);
            }
            free_slot = front;

            /*
             * Update front for the next insertion
             */ 
            front = (front+1) % capacity;

            /*
             * Found a slot.
             */
            struct map_entry mentry;
            mentry.valid = false;
            mentry.busy = true;
            mentry.index = free_slot;

            /*
             * Save the index in map
             */
            uuid_map.insert (std::pair<std::string, map_entry> (uid, mentry));
        
            return openarchive::success;

        }

        std::error_code fdcache_iopx::init_slot (file_ptr_t fp, req_ptr_t req,
                                                 uint32_t free_slot, 
                                                 uint32_t close_slot,
                                                 bool needs_close)
        { 
           
            if (needs_close) {
                assert (fd_queue[close_slot].valid == false);
                get_first_child()->close (fd_queue[close_slot].fp, req);
            }

            {
                mutex_guard_t guard (fd_queue[free_slot].op_mtx);

                if (fd_queue[free_slot].pending) { 

                    fd_queue[free_slot].fp = file_pool.make_shared ();

                    if (fd_queue[free_slot].fp) {

                        fd_queue[free_slot].fp->set_loc (fp->get_loc ());
                        openarchive::iopx_req::init_open_req (fd_queue[free_slot].fp, 
                                                              req, 
                                                              O_RDONLY | O_NOATIME); 

                        std::error_code ec = get_first_child()->open (fd_queue[free_slot].fp,
                                                                      req);
                        if (ec != ok) {
                            BOOST_LOG_FUNCTION ();
                            BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                           << " open failed for file " 
                                           << fd_queue[free_slot].fp->get_loc().get_pathstr()
                                           << " error code: " << errno
                                           << " error desc: " << strerror(errno);
                            return (ec);
                        }


                        ec = get_first_child ()->dup (fd_queue[free_slot].fp,
                                                      fp);
                        {
                            /*
                             * Now that all the required resources have been 
                             * initialized, we will update the entries in the 
                             * map
                             */
                            std::string uid = fp->get_loc ().get_uuidstr ();
                            std::map <std::string, map_entry>::iterator it;

                            wrlock_guard_t guard (&fdlock);

                            if (ec == ok) {
                                mark_vec_entry_ready (fd_queue[free_slot]);
                                it = uuid_map.find (uid);

                                if (it != uuid_map.end ()) {
                                    it->second.valid = true;
                                    it->second.busy = false;
                                }
                            }

                        } 

                        if (ec == ok) {
                            /*
                             * Set slot number to be used for future operations
                             */  
                            file_info_t info;
                            info.set_slot_num (free_slot);
                            fp->set_file_info (get_name (), info);

                            if (log_level >= openarchive::logger::level_debug_2) {
                                BOOST_LOG_FUNCTION ();
                                BOOST_LOG_SEV (log, 
                                               openarchive::logger::level_debug_2)
                                               << " created new fd-cache entry for "
                                               << fp->get_loc().get_pathstr()
                                               << " @ slot " << free_slot; 
                            }
                        }
 
                        return ec; 
                    }
    
                    /*
                     * Memory related problems ..
                     */  
                    return std::error_code (ENOMEM, std::generic_category());
                }
   
                file_ptr_t cache_fp;
                {
                    /*
                     * Revalidate whether the slot is valid and dup fd
                     */
                    rdlock_guard_t guard (&fdlock);
                    cache_fp = fd_queue[free_slot].fp;
                }

                if (cache_fp) {

                    std::error_code ec = get_first_child ()->dup (cache_fp, fp);
                    if (ec == ok) {
                        /*
                         * Set slot number to be used for future operations
                         */  
                        file_info_t info;
                        info.set_slot_num (free_slot);
                        fp->set_file_info (get_name (), info);
                    }

                    return ec;
                }

                return std::error_code (ENOENT, std::generic_category());
            }
                
        }

        std::error_code fdcache_iopx::get_fd (file_ptr_t fp)
        {
            std::string uid = fp->get_loc ().get_uuidstr ();
            uint32_t free_slot, close_slot;
            bool bclose;

            /*
             * Check whether a file with this UUID is already opened.
             * If so we will return this FD to the application.
             */
            std::error_code ec = search_fd (fp);
            if (ec == ok ) {
                return ec; 
            }

            req_ptr_t req = req_pool.make_shared ();

            if (ec.value () == ENOENT) {
                /*
                 * No file was found with matching uuid. We will allocate a 
                 * new fd with the provided uuid.
                 */ 
                ec = alloc_slot (fp, false, free_slot, close_slot, bclose); 
            }

            if (ec == ok) {
                /*
                 * A slot has been reserved in the circular buffer. We will
                 * perform the pending initializations.
                 */
                ec = init_slot (fp, req, free_slot, close_slot, bclose);
            }  

            return ec;
        }

        /*
         * All the required locks should have been taken before invoking
         * this function.
         */
        bool fdcache_iopx::is_validslot (req_ptr_t req, uint32_t slot)
        {
            off_t offset = req->get_offset ();
            size_t len = req->get_len ();
            off_t buff_offset = fd_queue[slot].rabuff.offset;
            size_t buff_len = fd_queue[slot].rabuff.bytes;

            if (fd_queue[slot].valid && fd_queue[slot].rabuff.valid) {
                if ((offset >= buff_offset) &&
                    (offset+len <= (buff_offset + buff_len))) {

                    return true;
                }
            } 

            return false;
        }

        plbuff_ptr_t fdcache_iopx::checkbuff (file_ptr_t fp, req_ptr_t req, 
                                              bool &eof)
        {
            file_info_t info;
            plbuff_ptr_t plbuff;

            eof = false;

            if (!fp->get_file_info (get_name(), info)) {
                /*
                 * fd-cache may not be enabled for this file.
                 */
                if (log_level >= openarchive::logger::level_error) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " fd-caching may not be enabled for "
                                   <<fp->get_loc ().get_pathstr ();
                }
                return plbuff;
            } 

            uint32_t slot = info.get_slot_num ();
            std::string uid = fp->get_loc ().get_uuidstr ();
        
            {
                rdlock_guard_t guard (&fdlock);

                if (fd_queue[slot].fp->get_loc ().get_uuidstr() == uid) {

                    /*
                     * The slot still contains the same file
                     */

                    if (req->get_offset() >= fd_queue[slot].fp->get_file_size()) {

                        /*
                         * EOF detected.
                         */ 
                        req->set_ret (0); 
                        eof = true;

                        if (log_level >= openarchive::logger::level_debug_2) {
                            BOOST_LOG_FUNCTION ();
                            BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                           << " EOF condition detected for "
                                           << req->get_fptr ()->get_loc().get_uuidstr()
                                           << " offset : " << req->get_offset ()
                                           << " length : " << req->get_len ();
                        }

                        /*
                         * Invoke the parent iopx pread_cbk method
                         */   
                        if (req->get_asyncio ()) { 
                            get_parent()->pread_cbk (fp, req, 
                                                     openarchive::success);
                        }

                        return plbuff;
                    }

                    if (is_validslot (req, slot)) {
                        /*
                         * The combination of offset and length can
                         * be satisfied by the current buffer contents.
                         */
                            
                        plbuff = fd_queue[slot].rabuff.plbuff; 
                        plbuff->set_offset (fd_queue[slot].rabuff.offset);
                        plbuff->set_bytes (fd_queue[slot].rabuff.bytes);
                
                        if (log_level >= openarchive::logger::level_debug_2) {
                            BOOST_LOG_FUNCTION ();
                            BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                                           << " found a valid entry "
                                           << fp->get_loc().get_pathstr()
                                           << " @ slot " << slot
                                           << " offset: " 
                                           << fd_queue[slot].rabuff.offset 
                                           << " len: " 
                                           << fd_queue[slot].rabuff.bytes;
                        }
                    }
                }
            }

            return plbuff; 

        }

        plbuff_ptr_t fdcache_iopx::getbuff (file_ptr_t fp, req_ptr_t req, 
                                            int32_t & res_slot)
        {
            /*
             * Check whether the file content exists in the read-ahead 
             * buffer
             */
            plbuff_ptr_t plbuff;
            uint32_t slot; 
            std::string uid = fp->get_loc ().get_uuidstr ();
            std::map <std::string, map_entry>::iterator it;
            bool reserve = false;

            {

                wrlock_guard_t guard (&fdlock);

                it = uuid_map.find (uid);

                if (it != uuid_map.end ()) {
                    if (it->second.valid) {
                        slot =  it->second.index;
                        if (fd_queue[slot].valid) {
                            if (is_validslot (req, slot)) {
                                /*
                                 * The combination of offset and length can
                                 * be satisfied by the current buffer
                                 * contents.
                                 */
                                
                                plbuff = fd_queue[slot].rabuff.plbuff; 
                                plbuff->set_offset (fd_queue[slot].rabuff.offset);
                                plbuff->set_bytes (fd_queue[slot].rabuff.bytes);

                                if (log_level >= openarchive::logger::level_debug_2) {
                                    BOOST_LOG_FUNCTION ();
                                    BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                                                   << " found a valid entry "
                                                   << fp->get_loc().get_pathstr()
                                                   << " @ slot " << slot
                                                   << " offset: " 
                                                   << fd_queue[slot].rabuff.offset 
                                                   << " len: " 
                                                   << fd_queue[slot].rabuff.bytes;
                                }
                            } else {
                                /*
                                 * No valid data in the slot to satisfy the 
                                 * combination of offset + size. More Data needs
                                 * to be read. We will make space for next 
                                 * read request and mark the read-ahead 
                                 * buf busy.
                                 */
                                if (!fd_queue[slot].rabuff.busy) {
                                    /*
                                     * rabuff.busy flag would be set if 
                                     * there is a read operation which 
                                     * is in progress. 
                                     */ 
                                    reserve_ra_buf (fd_queue[slot].rabuff);
                                } 
                                res_slot = slot;
                                reserve = true;
                            }
                        }
                    }  
                } 
            }  

            if (plbuff || reserve) {
                return plbuff;
            } 

            /*
             * No slot with matching uuid found.
             */
            res_slot = -1;
            return plbuff; 

        }

        std::error_code fdcache_iopx::readdata (file_ptr_t fp, req_ptr_t read_req,
                                                int32_t slot, bool & retry)
        {
            retry = false;
            /*
             * There is currently no data in the buffer which could service
             * the read request. We will fill the buffer with data, to service
             * future read requests.
             */
            req_ptr_t req = req_pool.make_shared ();
            std::string uid = fp->get_loc ().get_uuidstr ();
            std::error_code ec;

            if (slot < 0) {
                /*
                 * We will have to allocate a slot here
                 */
                uint32_t free_slot, close_slot;
                bool bclose;

                ec = alloc_slot (fp, true, free_slot, close_slot, bclose); 

                if (ec == ok) {
                    /*
                     * A slot has been reserved in the circular buffer. 
                     * We will perform the pending initializations.
                     */
                    ec = init_slot (fp, req, free_slot, close_slot, bclose);
                } else if (ec.value () == EALREADY || 
                           ec.value () == EADDRINUSE)  {
                    retry = true;
                } else {
                    retry = false;
                }

                if (ec == ok) {
                    slot = free_slot; 
                }  
            }

            if (slot < 0) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to find a slot for " 
                               << fp->get_loc ().get_pathstr () 
                               << " error code: " << errno 
                               << " error desc: " << strerror(errno);
                return ec;
            }

            if (read_req->get_asyncio ()) {
                return readdata_async (uid, fp, read_req, req, slot, retry);
            } else {
                return readdata_sync (uid, fp, read_req, req, slot, retry);
            } 
 
        }

        std::error_code fdcache_iopx::readdata_sync (std::string &uuid,
                                                     file_ptr_t fp, 
                                                     req_ptr_t read_req,
                                                     req_ptr_t req,
                                                     int32_t slot, bool &retry)
        {
            std::error_code ec;
            plbuff_ptr_t plbuff; 

            /*
             * We have a valid slot. We will read data to this slot.
             * Multiple threads might be trying to read data at the
             * same time. We will synchronize them using mutex. We 
             * can safely assume that the slot will not be released
             * since the rabuf.busy would have been set to true. 
             */
            mutex_guard_t guard (fd_queue[slot].rabuff.rd_mtx);

            if (fd_queue[slot].rabuff.pending) {
                /*
                 * Read is still pending. Issue a read request to 
                 * child iopx.
                 */  
                plbuff = buff_pool.make_shared ();
                if (plbuff) {
                    
                    struct iovec iov = { plbuff->get_base (), 
                                         plbuff->get_size () };

                    uint64_t offset = ra_bit_mask & read_req->get_offset();
                    init_read_req  (fd_queue[slot].fp, req, offset,
                                    plbuff->get_size (), 0, &iov);


                    std::error_code ec =  get_first_child () ->pread (fd_queue[slot].fp, 
                                                                      req);
                    if (ec != ok) {

                        BOOST_LOG_FUNCTION ();
                        BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                       << " pread failed for " 
                                       << fp->get_loc ().get_pathstr () 
                                       << " error code: " << errno 
                                       << " error desc: " << strerror(errno);
                        return ec; 
                    }

                    /*
                     * The data has been read. Now update the queue slot.
                     */
                    {
                        wrlock_guard_t guard (&fdlock);
                        fd_queue[slot].rabuff.offset = offset;
                        fd_queue[slot].rabuff.bytes = req->get_ret ();
                        fd_queue[slot].rabuff.plbuff = plbuff;
                        mark_ra_buf_ready (fd_queue[slot].rabuff);
                    }

                    plbuff->set_offset (req->get_offset ());
                    plbuff->set_bytes (req->get_ret ());

                    if (log_level >= openarchive::logger::level_debug_2) {
                        BOOST_LOG_FUNCTION ();
                        BOOST_LOG_SEV (log, openarchive::logger::level_debug_2)
                                       << " found a valid entry "
                                       << fp->get_loc().get_pathstr()
                                       << " @ slot " << slot
                                       << " offset: " << plbuff->get_offset ()
                                       << " len: " << plbuff->get_bytes ();
                    }

                    return processbuff (plbuff, fp, read_req);
                }

                return std::error_code (ENOMEM, std::generic_category ()); 
            }

            {
                std::string uid = fp->get_loc ().get_uuidstr (); 
                std::string slot_uid;

                rdlock_guard_t guard (&fdlock);

                slot_uid = fd_queue[slot].fp->get_loc ().get_uuidstr ();

                if (uid == slot_uid) {
                    if (is_validslot (read_req, slot)) {
                        return processbuff (fd_queue[slot].rabuff.plbuff, 
                                            fp, read_req);
                    } 
                }
            }

            /*
             * There is no pending read request so the request can be 
             * serviced by reattempting the read operation.
             */  
            retry = true;
            return std::error_code (EINPROGRESS, std::generic_category ());

        }

        std::error_code fdcache_iopx::readdata_async (std::string &uuid,
                                                      file_ptr_t fp, 
                                                      req_ptr_t read_req,
                                                      req_ptr_t req,
                                                      int32_t slot, bool &retry)
        {
            std::error_code ec;
            plbuff_ptr_t plbuff; 

            /*
             * We have a valid slot. We will read data to this slot.
             * Multiple threads might be trying to read data at the
             * same time. We will synchronize them using mutex. We 
             * can safely assume that the slot will not be released
             * since the rabuf.busy would have been set to true. 
             */
            mutex_guard_t guard (fd_queue[slot].rabuff.rd_mtx);

            if (fd_queue[slot].rabuff.pending &&
                !fd_queue[slot].rabuff.rd_in_progress) {

                /*
                 * Read is still pending and no read request has been 
                 * issused. Issue a read request to child iopx.
                 */  
                plbuff = buff_pool.make_shared ();
                if (plbuff) {
                        
                    struct iovec iov = { plbuff->get_base (), 
                                         plbuff->get_size () };

                    uint64_t offset = ra_bit_mask & read_req->get_offset();

                    init_read_req  (fd_queue[slot].fp, req, offset,
                                    plbuff->get_size (), 0, &iov);
                    req->set_asyncio (true);

                    ec = add_gen_req (uuid, slot, req, plbuff, read_req); 
                    if (ec != ok) {

                        BOOST_LOG_FUNCTION ();
                        BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                       << " failed to add to request map " 
                                       << fp->get_loc ().get_uuidstr () 
                                       << " error code: " << ec.value () 
                                       << " error desc: " 
                                       << strerror (ec.value ());
                        return ec; 
                    }

                    fd_queue[slot].rabuff.rd_in_progress = true;

                    ec =  get_first_child()->pread (fd_queue[slot].fp, req);
                    if (ec != ok) {

                        BOOST_LOG_FUNCTION ();
                        BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                       << " pread failed for "
                                       << fp->get_loc ().get_pathstr ()
                                       << " error code: " << ec.value ()
                                       << " error desc: "
                                       << strerror (ec.value ());

                        fd_queue[slot].rabuff.rd_in_progress = false;
                        del_req (uuid);

                        /*
                         * Invoke the parent pread callback handler.
                         */
                        req->set_ret (-1);
                        get_parent()->pread_cbk (read_req->get_fptr (), 
                                                 read_req, ec);
                            
                    }

                    return ec; 

                } else {

                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " failed to allocate memory for " 
                                   << fp->get_loc ().get_pathstr ();

                    return std::error_code (ENOMEM, std::generic_category ()); 
                } 

            }

            if (fd_queue[slot].rabuff.pending) {
                /*
                 * Add the request from parent iopx to waiting queue.
                 */
                ec = add_parent_req (uuid, read_req); 
                if (ec != ok) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " failed to add request for " 
                                   << fp->get_loc ().get_pathstr () 
                                   << " error code: " << errno 
                                   << " error desc: " << strerror(errno);
                }

                return ec; 
            } 

            /*
             * There is no pending read request so the request can be 
             * serviced by reattempting the read operation.
             */  
            retry = true;
            return std::error_code (EINPROGRESS, std::generic_category ());
        }  
                                                 
        std::error_code fdcache_iopx::processbuff (plbuff_ptr_t plbuff, 
                                                   file_ptr_t fp,
                                                   req_ptr_t req)
        {
            size_t len = req->get_len ();
            off_t offset = req->get_offset (); 
            uint64_t max_offset = plbuff->get_offset () + plbuff->get_bytes ();
            uint64_t max_bytes = max_offset - req->get_offset ();
            uint64_t bytes = ((len > max_bytes)? max_bytes : len);  
            void *buff = openarchive::iopx_req::get_buff_baseaddr (req);
            uint64_t delta_offset = offset - (plbuff->get_offset ());

            assert (buff != NULL);

            memcpy (buff, (char *)plbuff->get_base () + delta_offset,
                    bytes);  

            req->set_ret (bytes); 

            if (log_level >= openarchive::logger::level_debug_2) {

                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " callback is being invoked for " 
                               << fp->get_loc ().get_uuidstr () 
                               << " ret : " << req->get_ret ();
            }

            /*
             * Invoke the parent pread callback handler.
             */  
            if (req->get_asyncio ()) { 
                get_parent()->pread_cbk (req->get_fptr (), req, 
                                         openarchive::success);
            }

            return openarchive::success;
        } 

        std::error_code fdcache_iopx::add_gen_req (std::string & uuid,
                                                   uint32_t slot, 
                                                   req_ptr_t gen_req, 
                                                   plbuff_ptr_t plbuff,
                                                   req_ptr_t par_req)
        {
            rqmap_entry rqe; 

            rqe.gen_req = gen_req;
            rqe.slot = slot;
            rqe.plbuff = plbuff;
            rqe.parent_req.push (par_req);

            openarchive::arch_core::spinlock_handle handle(rqlock);

            /*
             * Check whether an entry already exists for the generated 
             * requests in the map. If an entry already exists then it
             * is an error.
             */   
            std::map <std::string, rqmap_entry>::iterator iter;
            iter = request_map.find (uuid);
            if (iter != request_map.end ()) {
                /*
                 * An entry already exists.
                 */
                return std::error_code (EALREADY, std::generic_category ()); 
            }

            request_map.insert (std::pair<std::string, rqmap_entry> (uuid, 
                                                                     rqe));

            return openarchive::success;

        }

        std::error_code fdcache_iopx::add_parent_req (std::string & uuid, 
                                                      req_ptr_t req)
        {
            std::map <std::string, rqmap_entry>::iterator iter;
       
            openarchive::arch_core::spinlock_handle handle(rqlock);

            iter = request_map.find (uuid);
            if (iter == request_map.end ()) {
                /*
                 * No entry exists.
                 */
                return std::error_code (ENOENT, std::generic_category ()); 
            }

            iter->second.parent_req.push (req);
            return openarchive::success;
        }

        std::error_code fdcache_iopx::del_req (std::string & uuid)
        {
            std::map <std::string, rqmap_entry>::iterator iter;

            openarchive::arch_core::spinlock_handle handle(rqlock);

            iter = request_map.find (uuid);
            if (iter == request_map.end ()) {
                /*
                 * No entry exists.
                 */
                return std::error_code (ENOENT, std::generic_category ()); 
            }

            request_map.erase (uuid);
            return openarchive::success;
        }

        std::error_code fdcache_iopx::open (file_ptr_t fp, req_ptr_t req)
        {
            if (req->get_flags() & O_WRONLY || req->get_flags () & O_RDWR) {

                return (get_first_child ()->open (fp, req));

            } else {

                /*
                 * The file is being opened in read-only mode. fd-caching 
                 * can be enabled for this file.
                 */
                for(uint32_t attempt = 0; attempt < 3; attempt++) {

                    std::error_code ec = get_fd (fp);
                    if (ec == ok) {
                        return ec;
                    }

                    if (ec.value () == EALREADY || ec.value () == EADDRINUSE) {
                        /*
                         * This could happen if multiple threads are trying to 
                         * get fd for same uuid. In that case the first thread 
                         * would succeed, while the others would fail with 
                         * EALREADY or EADDRINUSE. We will try to search 
                         * whether the fd is already correctly set by the 
                         * other thread.
                         */
                        continue;  
                    } else {
                        break;
                    }
                }
            }

            if (log_level >= openarchive::logger::level_error) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " fd-cache could not be enabled for "
                               << fp->get_loc().get_pathstr();
            }

            return (get_first_child ()->open (fp, req));

        }

        std::error_code fdcache_iopx::pread (file_ptr_t fp, req_ptr_t req)
        {

            /*
             * Check whether there is an existing buffer which could
             * satisfy the request.
             */
            bool retry = false;
            bool eof = false; 
   
            do {
                plbuff_ptr_t plbuff = checkbuff (fp, req, eof);
                if (eof) {
                    return openarchive::success;
                }

                if (plbuff) {
                    return processbuff (plbuff, fp, req);    
                }
            
                int32_t slot;
                plbuff = getbuff (fp, req, slot);
                if (plbuff) {
                    return processbuff (plbuff, fp, req);    
                }   

                std::error_code ec = readdata (fp, req, slot, retry);
                if (ec == ok) {
                    return ec;
                }

            } while (retry); 

            /*
             * The read request cannot be satisfied by the current contents
             * of read buffer. Flag an error.
             */  
            if (log_level >= openarchive::logger::level_error) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " failed to process read for "
                               << fp->get_loc ().get_uuidstr (); 
            }

            return std::error_code (ENOENT, std::generic_category ());
        }

        std::error_code fdcache_iopx::pread_cbk (file_ptr_t fp, req_ptr_t req,
                                                 std::error_code ec)
        {
            /*
             * Callback handler has been invoked for one of the read requests.
             * Update the read-ahead buffer and call callbacks for any 
             * outstanding read requests from parent iopx.
             */
            std::map <std::string, rqmap_entry>::iterator iter;
            std::string uuid = fp->get_loc ().get_uuidstr ();

            if (log_level >= openarchive::logger::level_debug_2) {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " callback invoked for "
                               << fp->get_loc().get_uuidstr()
                               << " offset : " << req->get_offset ()
                               << " ret : " << req->get_ret ();
            }

            {
                openarchive::arch_core::spinlock_handle handle(rqlock);

                iter = request_map.find (uuid);
                if (iter == request_map.end ()) {
                    /*
                     * No entry exists.
                     */
                    return std::error_code (ENOENT, std::generic_category ()); 
                }
            }

            uint32_t slot = iter->second.slot;

            /*
             * The data has been read. Now update the queue slot.
             */
            {
                wrlock_guard_t guard (&fdlock);
                fd_queue[slot].rabuff.offset = req->get_offset ();
                fd_queue[slot].rabuff.bytes = req->get_ret ();
                fd_queue[slot].rabuff.plbuff = iter->second.plbuff;
                mark_ra_buf_ready (fd_queue[slot].rabuff);
                iter->second.plbuff->set_offset (req->get_offset());
                iter->second.plbuff->set_bytes (req->get_ret ());
            }

            /*
             * Now that the slot has been updated, we will start processing 
             * the requests from parent iopx.
             */
            req_ptr_t par_req;
              
            while(iter->second.parent_req.empty() == false) {
                par_req = iter->second.parent_req.front ();
                iter->second.parent_req.pop ();

                if (log_level >= openarchive::logger::level_debug_2) {
                    BOOST_LOG_FUNCTION ();
                    BOOST_LOG_SEV (log, openarchive::logger::level_error)
                                   << " processbuff will be invoked for "
                                   << par_req->get_fptr ()->get_loc().get_uuidstr()
                                   << " offset : " << par_req->get_offset ()
                                   << " length : " << par_req->get_len ();
                }

                processbuff (iter->second.plbuff, par_req->get_fptr(), par_req);
            }

            /*
             * Now that the request has been processed, delete the entry from 
             * request map.
             */  
            del_req (uuid);

            return openarchive::success;
        }
 
        void fdcache_iopx::profile (void)
        {
            std::string stats;

            file_pool.getstats (stats);
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << stats;
            }

            req_pool.getstats (stats);
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << stats;
            }

            buff_pool.getstats (stats);
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << stats;
            }

            get_first_child ()->profile ();

            return;
        } 

    }
}
