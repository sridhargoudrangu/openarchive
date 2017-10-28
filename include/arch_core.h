/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __ARCH_CORE_H__
#define __ARCH_CORE_H__

#include <pthread.h>
#include <semaphore.h>
#include <string.h>
#include <sys/uio.h>
#include <cerrno>
#include <atomic>
#include <system_error>
#include <exception>
#include <queue>
#include <iterator>
#include <map>
#include <chrono>
#include <thread>
#include <mutex>
#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/algorithm/string/trim_all.hpp>
#include <boost/tokenizer.hpp>
#include <boost/lexical_cast.hpp>
#include <arch_loc.h>

typedef boost::tokenizer<boost::char_separator<char>> tokenizer_t;
typedef std::map <std::string, std::string> params_map_t; 

namespace openarchive
{
    /* 
     * default is success 
     */
    const std::error_code success (0, std::generic_category ());  
    const std::error_condition ok;

    /*
     * typedefs to make life easier across the module.
     */
    typedef boost::shared_ptr<openarchive::arch_loc::arch_loc> loc_ptr_t;

    typedef std::atomic<int32_t >            atomic_int32_t; 
    typedef std::atomic<uint32_t>            atomic_uint32_t; 
    typedef std::atomic<int64_t >            atomic_int64_t; 
    typedef std::atomic<uint64_t>            atomic_uint64_t; 
    typedef std::atomic<bool>                atomic_bool;  

    typedef volatile std::atomic<int32_t >   atomic_vol_int32_t; 
    typedef volatile std::atomic<uint32_t>   atomic_vol_uint32_t; 
    typedef volatile std::atomic<int64_t >   atomic_vol_int64_t; 
    typedef volatile std::atomic<uint64_t>   atomic_vol_uint64_t; 
    typedef volatile std::atomic<bool>       atomic_vol_bool;

    /*
     * types of data managment operations supported
     */
    enum arch_op_type
    {
        BACKUP = 1,
        RESTORE = 2,
        ARCHIVE = 3
    };   
  

    namespace arch_core
    {

        class arch_exception: public std::exception
        {
            std::error_code ec;

            public:
            arch_exception (std::error_code e): ec(e) {   }

            virtual const char* what() const throw()
            {
                return (strerror (ec.value()));
            }
        }; 

        class bitops
        {
            public: 
            static inline uint32_t num_bits_set (uint64_t val)
            {
                uint32_t bits=0;
         
                while(val) {
                    if (val&0x0001ul) {
                        bits++;
                    }
                    val=val >> 1;
                }

                return(bits);
            }

            static inline uint32_t bit_width (uint64_t val)
            {
                uint32_t bits=0;
         
                while(!(val&0x0001ul)) {

                    val=val >> 1;
                    bits++;

                }

                return(bits);
            }
        };

        class data_mgmt_stats
        {
            atomic_vol_uint64_t pending;
            atomic_vol_uint64_t bytes;
            atomic_bool done; 
            uint64_t req_size; 
        
            public:

            data_mgmt_stats (void)
            {
                pending.store (0);
                bytes.store (0);
                req_size = 0;
                done.store (false);
            }
 
            void clear_pending (void) { pending.store (0); }
            void clear_bytes (void) { bytes.store (0); }
            void clear_done (void) { done.store (false); }
            void set_done (void) { done.store (true); }
            bool get_done (void) { return done.load (); } 

            uint64_t incr_pending (uint64_t val)
            {
                pending.fetch_add (val);
                return pending.load ();
            }   

            uint64_t decr_pending (uint64_t val)
            {
                pending.fetch_sub (val);
                return pending.load ();
            }   

            uint64_t incr_bytes (uint64_t val)
            {
                bytes.fetch_add (val);
                return bytes.load ();
            }   

            void set_req_size (uint64_t val)
            {
                req_size = val;
                return;
            }

            uint64_t get_req_size (void)
            {
                return req_size; 
            } 

        };

        class spinlock
        {
            pthread_spinlock_t splock;
            bool ready;
            int last_errno;

            public:
            spinlock (void): ready (false)
            {
                int ret = pthread_spin_init (&splock, PTHREAD_PROCESS_PRIVATE);
                if (ret) {
                    last_errno = errno;
                } else {
                    ready = true; 
                }
            }

            ~spinlock (void)
            {
                if (ready) {
                    pthread_spin_destroy (&splock); 
                }
            }

            std::error_code lock (void)
            {
                if (!ready) {
                    std::error_code ec (last_errno, std::generic_category ());
                    return ec;
                }

                if (pthread_spin_lock (&splock)) {
                    last_errno = errno;
                    std::error_code ec (errno, std::generic_category ());
                    return ec;
                }             

                return (openarchive::success);
 
            }

            std::error_code unlock (void)
            {
                if (!ready) {
                    std::error_code ec (last_errno, std::generic_category ());
                    return ec;
                }

                if (pthread_spin_unlock (&splock)) {
                    last_errno = errno;
                    std::error_code ec (errno, std::generic_category ());
                    return ec;
                }             

                return (openarchive::success);
            }

            std::error_code get_errcode (void) 
            {  
                std::error_code ec (errno, std::generic_category ());
                return ec;
            }

        };

        class spinlock_handle
        {
            spinlock &ref;

            public:
            spinlock_handle (spinlock &sl): ref (sl)
            {
                std::error_code ec = ref.lock ();  
                if (ec != ok) {
                    throw (arch_exception (ec));
                }  
            }

            ~spinlock_handle(void)
            {
                ref.unlock ();
            } 
        };

        template <class T> class mtqueue
        {
            std::queue<T> que;
            spinlock lock;

            public:
            bool push(T &t)
            {
                spinlock_handle handle (lock);
                que.push (t);   
                return true;
            }

            bool pop (T &t)
            {
                spinlock_handle handle (lock);
                if (que.empty ()) {
                    return false;
                }

                t = que.front ();
                que.pop ();
                return true;    
            } 

            bool empty (void)
            {
                spinlock_handle handle (lock);
                return (que.empty ()); 
            } 

        };

        template <class X, class Y> class mtmap
        {
            std::map<X, Y> map;
            spinlock lock;

            public:
            bool insert (X name, Y val)
            {
                spinlock_handle handle (lock);
                
                std::pair< typename std::map<X, Y>::iterator, bool> ret;
                ret = map.insert (std::pair<X, Y>(name, val));
                
                return ret.second; 
            } 

            bool extract (X name, Y &val)
            {
                spinlock_handle handle (lock);

                typename std::map<X, Y>::iterator it;

                it = map.find (name);
                if (it != map.end()) {
                    val = it->second;
                    return true; 
                }

                return false;
            }

            void erase (X name)
            {
                spinlock_handle handle (lock);
                map.erase (name);

                return;
            }

            bool atomic_increment (X name, Y val)
            {
                spinlock_handle handle (lock);

                typename std::map<X, Y>::iterator it;

                it = map.find (name);
                if (it != map.end()) {
                    it->second += val;
                    return true;
                }
 
                return false; 
            }

            bool atomic_decrement (X name, Y val)
            {
                spinlock_handle handle (lock);

                typename std::map<X, Y>::iterator it;

                it = map.find (name);
                if (it != map.end()) {
                    it->second -= val;
                    return true;
                }
 
                return false; 
            }

            bool increment (X name, Y val)
            {
                typename std::map<X, Y>::iterator it;

                it = map.find (name);
                if (it != map.end()) {
                    it->second += val;
                    return true;
                }
 
                return false; 
            }

            bool decrement (X name, Y val)
            {
                typename std::map<X, Y>::iterator it;

                it = map.find (name);
                if (it != map.end()) {
                    it->second -= val;
                    return true;
                }
 
                return false; 
            }

        };

        class popen
        {
            std::string cmd;
            std::vector<std::string> output;
            FILE *pfile;  
            bool valid;

            public:
            popen (std::string & str, uint32_t del): cmd(str), pfile(NULL), 
                                                     valid(false)
            {
                if (cmd.length ()) {
                    pfile = ::popen (cmd.c_str(), "r");
                }      
                
                if (!pfile)  {
                    return; 
                }

                if (del) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(del));
                }

                char cbuff[4096];

                while(fgets (cbuff, sizeof(cbuff), pfile)) {
                    
                    std::string out(cbuff);
                    output.push_back (boost::trim_all_copy (out));
                     
                }

                if (pfile) {
                    ::pclose(pfile);
                    pfile = NULL; 
                }

                valid = true;
                
            }

            ~popen (void)
            {
                if (pfile) {
                    ::pclose(pfile);
                    pfile = NULL; 
                }
            }

            bool is_valid (void) { return valid; }

            std::vector<std::string> & get_output (void)
            {
                return output;
            }
   
        };

        
        template <class T> class lock_guard
        {
            T lk;

            public:
            lock_guard (T t): lk (t)
            {
                lk->lock ();
            }

            ~lock_guard(void)
            {
                lk->unlock ();
            } 
        };

        class semaphore
        {

            bool valid;
            sem_t sem;

            public:
            semaphore (int count = 0): valid (false)
            {
                if (!sem_init (&sem, 0, count)) {
                    valid = true;
                }
            }

            ~semaphore (void)
            {
                if (valid) {
                    sem_destroy (&sem); 
                }
            }

            int32_t post (void)
            {
                if (!valid) {
                    return -1; 
                } 

                return sem_post (&sem);
            }

            int32_t wait (void)
            {
                if (!valid) {
                    return -1; 
                } 

                return sem_wait (&sem);
            }
        };

        class sem_lock_guard
        {
            semaphore &ref;

            public:
            sem_lock_guard (semaphore &r): ref (r)
            {
                ref.wait ();
            }

            ~sem_lock_guard(void)
            {
                ref.post ();
            } 
        };

        class file_tracker
        {
            std::string file_path;
            std::ofstream outpstream;
            std::mutex lock;

            public:
            file_tracker (std::string path): file_path (path)
            {
                outpstream.open (file_path.c_str ());  
                return;  
            }

            ~file_tracker (void)
            {
                outpstream.close ();
            }

            std::error_code append (std::string & entry)
            {
                std::lock_guard<std::mutex> guard(lock);
                if (outpstream.good ()) {
                    outpstream << entry <<std::endl;
                    return openarchive::success; 
                }

                std::error_code ec (EBADFD, std::generic_category());
                return ec;
            }

            bool good (void) { return outpstream.good(); }
        };

        class rwlock
        {
            pthread_rwlock_t lock;

            public:
            rwlock (void)
            {
                pthread_rwlock_init (&lock, NULL);
            }

            ~rwlock (void)
            {
                pthread_rwlock_destroy (&lock);
            }

            int rdlock (void)
            {
                return pthread_rwlock_rdlock (&lock);
            }
 
            int wrlock (void)
            {
                return pthread_rwlock_wrlock (&lock);
            }

            int unlock (void)
            {
                return pthread_rwlock_unlock (&lock);
            } 
        };   

        class rdlock_guard
        {
            rwlock * lock;

            public:
            rdlock_guard (rwlock *ptr):lock(ptr)
            {
                lock->rdlock ();
            } 
            ~rdlock_guard (void)
            {
                lock->unlock ();
            } 
        };
 
        class wrlock_guard
        {
            rwlock * lock;

            public:
            wrlock_guard (rwlock *ptr):lock(ptr)
            {
                lock->wrlock ();
            } 
            ~wrlock_guard (void)
            {
                lock->unlock ();
            } 
        };

        class arg_parser
        {
            std::string args;
            params_map_t args_map; 

            public:
            arg_parser (std::string &inp): args(inp)
            {
                /*
                 * Parse the supplied arguments string and extract the 
                 * parameters. The argument string is expected to be in 
                 * the following format.
                 * "cc=2:cn=node1:ph=node2:pp=8400:at=29:in=Instance001:bs=idm:"
                 * "sc=arch:ji=124:jt=full-backup:ns=16"
                 */ 
                boost::char_separator<char> sep{":"};
                tokenizer_t tok{args, sep};

                for(tokenizer_t::iterator it = tok.begin (); it != tok.end (); 
                    it++) {
                    std::string kv = *it;
                    /*
                     * Extract key/value pair from the <key:value> string
                     */
                    std::string key;
                    std::string value;

                    if (ok == parse_kv_pair (kv, key, value)) {
                        args_map[key] = value;
                    }
                }  

            }

            std::error_code parse_kv_pair (std::string &kv, 
                                           std::string &key,
                                           std::string &value)
            {
                boost::char_separator<char> sep{"="};
                tokenizer_t tok{kv, sep};

                tokenizer_t::iterator it = tok.begin (); 

                if (it == tok.end ()) {
                    return (std::error_code (EINVAL, std::generic_category()));
                }

                key = *it;
                it++;  
     
                if (it == tok.end ()) {
                    return (std::error_code (EINVAL, std::generic_category()));
                }
                
                value = *it; 
                return openarchive::success;
            }

            std::error_code extract_param (std::string label, std::string &val)
            {
                std::map<std::string, std::string>::iterator it;
                it = args_map.find (label);
                if (it == args_map.end ()) {
                    return (std::error_code (ENOENT, std::generic_category()));
                }

                val = it->second; 
                return openarchive::success;
            }

        };

    } /* End of arch_core */

    typedef openarchive::arch_core::file_tracker file_tracker_t; 
    typedef boost::shared_ptr <file_tracker_t> file_tracker_ptr_t;

    typedef openarchive::arch_core::data_mgmt_stats  dmstats_t;
    typedef boost::shared_ptr<dmstats_t>             dmstats_ptr_t;  

    typedef openarchive::arch_core::rwlock rwlock_t;
    typedef openarchive::arch_core::rdlock_guard rdlock_guard_t;
    typedef openarchive::arch_core::wrlock_guard wrlock_guard_t;

} /* End of openarchive */
 
#endif /* End of __ARCH_CORE_H__ */
