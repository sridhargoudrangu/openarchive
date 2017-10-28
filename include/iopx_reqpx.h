/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <atomic>
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/variant.hpp>
#include <arch_file.h>
#include <arch_mem.hpp>

#ifndef __IOPX_REQPX_H__
#define __IOPX_REQPX_H__

namespace openarchive
{
    typedef boost::shared_ptr<struct stat>                         stat_ptr_t;
    typedef boost::shared_ptr<boost::asio::io_service>             io_service_ptr_t;
    typedef boost::shared_ptr<boost::asio::io_service::work>       work_ptr_t; 
    typedef boost::shared_ptr<openarchive::arch_mem::buff>         buff_ptr_t;

    namespace iopx_req
    {

        enum fop_type
        {
            OPEN_FOP       =  1,
            CLOSE_FOP      =  2,
            PREAD_FOP      =  3,
            PWRITE_FOP     =  4,
            FSTAT_FOP      =  5,
            STAT_FOP       =  6,
            FTRUNCATE_FOP  =  7,
            TRUNCATE_FOP   =  8,
            FSETX_FOP      =  9,
            SETX_FOP       =  10,
            FGETX_FOP      =  11,
            GETX_FOP       =  12, 
            REMOVEX_FOP    =  13,
            FREMOVEX_FOP   =  14,
            LSEEK_FOP      =  15,
            MKDIR_FOP      =  16,
            GETUUID_FOP    =  17,
            RESOLVE_FOP    =  18,  
            GETHOSTS_FOP   =  19,
            SCAN_FOP       =  20, 
            UNDEF_FOP      =  127   
        };

        enum data_type
        {
            POI_DATA      =  1,
            BUFF_DATA     =  2,
            POD_DATA      =  3,
            XTBUFF_DATA   =  4,
            POS_DATA      =  5,
            STAT_DATA     =  6,
            RESOLVE_DATA  =  7,
            GETHOSTS_DATA =  8, 
            STR_DATA      =  9,  
            UNDEF_DATA    =  127
        };

        class fop_data
        {
            data_type     type;
            boost::variant<struct iovec *, buff_ptr_t, void *, 
                           struct stat *, stat_ptr_t,
                           std::list <arch_loc_t> *, 
                           std::list <std::string> *, 
                           std::string *> val;
 
            public:
            void set_poi    (struct iovec * v)   
            {
                val        =  v;
                type       =  POI_DATA;
            }

            void set_bufp   (buff_ptr_t p)       
            {
                val        =  p;
                type       =  BUFF_DATA;
            }

            void set_pod    (void * d)           
            {
                val        =  d;
                type       =  POD_DATA;
            }

            void set_xtbufp (buff_ptr_t p)       
            {
                val        =  p;
                type       =  XTBUFF_DATA;
            }

            void set_pos    (struct stat * p)    
            {
                val        =  p;
                type       =  POS_DATA;
            }    

            void set_pstat  (stat_ptr_t p)       
            {
                val        =  p;
                type       =  STAT_DATA;
            }

            void set_ploc (std::list <arch_loc_t> * p)
            {
                val        =  p;
                type       =  RESOLVE_DATA; 
            }

            void set_phosts (std::list <std::string> * p)
            {
                val        =  p;
                type       =  GETHOSTS_DATA; 
            } 

            void set_str (std::string * p)
            {
                val        =  p;
                type       =  STR_DATA; 
            }

            struct iovec *  get_poi    (void)    
            {
                assert (type == POI_DATA);
                return boost::get<struct iovec *> (val);
            }

            buff_ptr_t      get_bufp   (void)    
            {
                assert (type == BUFF_DATA);
                return boost::get<buff_ptr_t> (val);
            }
                                         
            void *          get_pod    (void)    
            {
                assert (type == POD_DATA);
                return boost::get<void *> (val);
            }

            buff_ptr_t      get_xtbufp (void)    
            {
                assert (type == XTBUFF_DATA);
                return boost::get<buff_ptr_t> (val);
            }

            struct stat  *  get_pos    (void)    
            {
                assert (type == POS_DATA);
                return boost::get<struct stat *> (val);
            }

            stat_ptr_t      get_pstat  (void)    
            {
                assert (type == STAT_DATA);
                return boost::get<stat_ptr_t> (val);
            }

            std::list <arch_loc_t> * get_ploc (void)
            {
                assert (type ==  RESOLVE_DATA);
                return boost::get<std::list <arch_loc_t> *> (val);
            }

            std::list <std::string> * get_phosts (void)
            {
                assert (type ==  GETHOSTS_DATA);
                return boost::get<std::list <std::string> *> (val);
            }

            std::string * get_str (void)
            {
                assert (type == STR_DATA);
                return boost::get<std::string *> (val);    
            }
                                                 
        };


        class iopx_req
        {
            file_ptr_t      fptr;
            fop_type        ftype;
            data_type       dtype;
            fop_data        data;
            std::string     desc;
            std::string     info;
            uint64_t        len; 
            uint64_t        flags;
            uint64_t        offset;   
            int64_t         ret;
            bool            async_io;
            std::error_code code;
            openarchive::arch_core::mtmap<std::string, int64_t>   rcmap;
            openarchive::arch_core::mtmap<std::string, uint64_t>  childcount; 
            openarchive::arch_core::mtmap<std::string, uint64_t>  respcount; 
            openarchive::arch_core::mtmap<std::string, uint64_t>  idmap;

            public:
            iopx_req (void) 
            {
                ftype = UNDEF_FOP;
                dtype = UNDEF_DATA;
                len = 0;
                offset = 0;
                ret = -1;
                async_io = false;
            }

            void set_fptr   (file_ptr_t fp)      { fptr = fp;                 }
            void set_ftype  (fop_type t)         { ftype = t;                 }
            void set_len    (uint64_t l)         { len = l;                   }
            void set_ret    (int64_t r)          { ret = r;                   }
            void set_flags  (uint64_t f)         { flags = f;                 }
            void set_offset (uint64_t o)         { offset = o;                }
            void set_ec     (std::error_code ec) { code = ec;                 } 
            void set_desc   (std::string &s)     { desc = s;                  }
            void set_info   (std::string &i)     { info = i;                  }
            void set_asyncio(bool b)             { async_io = b;              }

            void set_poi    (struct iovec * v)   
            {
                data.set_poi (v);
                dtype = POI_DATA;
            }

            void set_bufp   (buff_ptr_t p)       
            { 
                data.set_bufp (p);
                dtype = BUFF_DATA;       
            }

            void set_pod    (void * d)           
            {
                data.set_pod (d);
                dtype = POD_DATA;
            } 

            void set_xtbufp (buff_ptr_t p)       
            { 
                data.set_xtbufp (p);
                dtype = XTBUFF_DATA;
            }

            void set_pos    (struct stat * p)    
            {
                data.set_pos (p);
                dtype = POS_DATA;
            }    

            void set_pstat  (stat_ptr_t p)       
            { 
                data.set_pstat (p); 
                dtype = STAT_DATA;       
            }

            void set_ploc (std::list <arch_loc_t> * p)
            {
                data.set_ploc (p);
                dtype = RESOLVE_DATA;
            }

            void set_phosts (std::list <std::string> * p)
            {
                data.set_phosts (p);
                dtype = GETHOSTS_DATA;
            }

            void set_str (std::string * p)
            {
                data.set_str (p);
                dtype = STR_DATA;
            }

            bool set_retcode (std::string, int64_t); 
            bool set_childcount (std::string, uint64_t);
            bool set_id (std::string, uint64_t); 
            bool post_resp (std::string); 

            bool erase_id (std::string);
 
            file_ptr_t       get_fptr   (void)   { return fptr;               }
            fop_type         get_ftype  (void)   { return ftype;              }
            data_type        get_dtype  (void)   { return dtype;              }
            uint64_t         get_len    (void)   { return len;                }
            uint64_t         get_offset (void)   { return offset;             }
            int64_t          get_ret    (void)   { return ret;                }
            std::error_code  get_ec     (void)   { return code;               }
            uint64_t         get_flags  (void)   { return flags;              }
            std::string &    get_desc   (void)   { return desc;               }
            std::string &    get_info   (void)   { return info;               }
            bool             get_asyncio(void)   { return async_io;           }

            struct iovec *   get_poi    (void)   
            {
                assert (dtype == POI_DATA);
                return data.get_poi();
            }

            buff_ptr_t       get_bufp   (void)   
            { 
                assert (dtype == BUFF_DATA);
                return data.get_bufp ();  
            }
                                         
            void *           get_pod    (void)   
            {
                assert (dtype == POD_DATA);
                return data.get_pod();
            }

            buff_ptr_t       get_xtbufp (void)   
            { 
                assert (dtype == XTBUFF_DATA);
                return data.get_xtbufp (); 
            }

            struct stat  *   get_pos    (void)   
            {
                assert (dtype == POS_DATA);
                return data.get_pos();
            }

            stat_ptr_t       get_pstat  (void)   
            { 
                assert (dtype == STAT_DATA);
                return data.get_pstat ();  
            }

            std::list <arch_loc_t> * get_ploc (void)
            {
                assert (dtype == RESOLVE_DATA);
                return data.get_ploc ();  
            } 

            std::list <std::string> * get_phosts (void)
            {
                assert (dtype == GETHOSTS_DATA);
                return data.get_phosts ();  
            } 
            
            std::string * get_str (void)
            {
                assert (dtype == STR_DATA);
                return data.get_str ();
            }

            bool get_retcode (std::string, int64_t &);
            bool get_childcount (std::string, uint64_t &);
            bool get_id (std::string, uint64_t &); 
            bool get_resp (std::string, uint64_t &);  
        };

        void         *  get_buff_baseaddr   (boost::shared_ptr<iopx_req>);
        void         *  get_xtattr_baseaddr (boost::shared_ptr<iopx_req>);
        struct stat  *  get_stat_baseaddr   (boost::shared_ptr<iopx_req>);

        void init_fstat_req (file_ptr_t, 
                             boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                             struct stat *); 

        void init_stat_req  (file_ptr_t, 
                             boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                             struct stat *); 

        void init_open_req  (file_ptr_t, 
                             boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                             uint64_t);

        void init_creat_req (file_ptr_t, 
                             boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                             uint64_t, uint64_t);

        void init_close_req (file_ptr_t fp,
                             boost::shared_ptr<openarchive::iopx_req::iopx_req>);

        void init_read_req  (file_ptr_t,
                             boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                             uint64_t, uint64_t, uint64_t, struct iovec *);

        void init_read_req  (file_ptr_t,
                             boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                             uint64_t, uint64_t, uint64_t, buff_ptr_t);

        void init_read_req  (file_ptr_t, 
                             boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                             uint64_t, uint64_t, uint64_t, void *);

        void init_write_req (file_ptr_t,
                             boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                             uint64_t, uint64_t, uint64_t, struct iovec *);

        void init_write_req (file_ptr_t,
                             boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                             uint64_t, uint64_t, uint64_t, buff_ptr_t);

        void init_fsetxattr_req (file_ptr_t,
                                 boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                                 std::string, struct iovec *, uint64_t); 

        void init_setxattr_req (file_ptr_t,
                                boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                                std::string, struct iovec *, uint64_t); 

        void init_fgetxattr_req (file_ptr_t,
                                 boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                                 std::string, struct iovec *);

        void init_getxattr_req (file_ptr_t,
                                boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                                std::string, struct iovec *);

        void init_fremovexattr_req (file_ptr_t,
                                    boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                                    std::string);

        void init_removexattr_req (file_ptr_t,
                                   boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                                   std::string);

        void init_mkdir_req (file_ptr_t, 
                             boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                             uint64_t);

        void init_ftruncate_req (file_ptr_t, 
                                 boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                                 uint64_t); 

        void init_getuuid_req (file_ptr_t,
                               boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                               struct iovec *);

        void init_resolve_req (file_ptr_t,
                               boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                               std::list <arch_loc_t> *, uint64_t);

        void init_gethosts_req (file_ptr_t,
                                boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                                std::list <std::string> *);

        void init_scan_req (file_ptr_t,
                            boost::shared_ptr<openarchive::iopx_req::iopx_req>,
                            std::string *, archstore_scan_type_t);

    } /* End of namespace iopx_req */

    typedef openarchive::iopx_req::iopx_req     req_t;
    typedef boost::shared_ptr<req_t>            req_ptr_t;

} /* End of namespace openarchive */
#endif /* End of __IOPX_REQPX_H__ */
