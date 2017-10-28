/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <iopx_reqpx.h>

namespace openarchive
{
    namespace iopx_req
    {
        bool iopx_req::set_retcode (std::string name, int64_t value)
        {
            return rcmap.insert (name, value);
        }

        bool iopx_req::set_childcount (std::string name, uint64_t count)
        {
            return childcount.insert (name, count); 
        }

        bool iopx_req::set_id (std::string name, uint64_t id)
        {
            return idmap.insert (name, id); 
        }

        bool iopx_req::post_resp (std::string name)
        {
            return respcount.atomic_increment (name, 1);
        }

        bool iopx_req::erase_id (std::string name)
        {
            idmap.erase (name);
            return true; 
        }

        bool iopx_req::get_retcode (std::string name, int64_t & ret)
        {
            return rcmap.extract (name, ret); 
        }

        bool iopx_req::get_childcount (std::string name, uint64_t & count)
        {
            return childcount.extract (name, count); 
        }

        bool iopx_req::get_id (std::string name, uint64_t & id)
        {
            return idmap.extract (name, id); 
        }

        bool iopx_req::get_resp (std::string name, uint64_t & count)
        {
            return respcount.extract (name, count); 
        }
  
        void * get_buff_baseaddr (boost::shared_ptr<iopx_req> req)
        {
            void *buff = NULL;
            openarchive::iopx_req::data_type dtype = req->get_dtype (); 

            if (dtype == openarchive::iopx_req::BUFF_DATA) {

                buff_ptr_t buffp = req->get_bufp ();
                buff = buffp->get_base (); 
             
            } else if (dtype == openarchive::iopx_req::POI_DATA) {
       
                struct iovec *iov = req->get_poi ();
                buff = iov->iov_base;

            } else if (dtype == openarchive::iopx_req::POD_DATA) {

                buff = req->get_pod ();
            }

            return buff;
        }

        void * get_xtattr_baseaddr (boost::shared_ptr<iopx_req> req)
        {
            void *buff = NULL;
            openarchive::iopx_req::data_type dtype = req->get_dtype (); 

            if (dtype == openarchive::iopx_req::XTBUFF_DATA) {

                buff_ptr_t xtbuff = req->get_xtbufp ();
                buff = xtbuff->get_base (); 
             
            } else if (dtype == openarchive::iopx_req::POI_DATA) {

                struct iovec *iov = req->get_poi ();
                buff = iov->iov_base;
            }

            return buff;
        }

        struct stat * get_stat_baseaddr (boost::shared_ptr<iopx_req> req)
        {
            struct stat * statp = NULL;
            openarchive::iopx_req::data_type dtype = req->get_dtype (); 

            if (openarchive::iopx_req::POS_DATA == dtype) {

                statp = req->get_pos ();

            } else if (openarchive::iopx_req::STAT_DATA == dtype) {

                stat_ptr_t p = req->get_pstat();
                statp = p.get();

            }

            return statp;
        }

        void init_stat_req (file_ptr_t fp, req_ptr_t req, struct stat *ptr)
        {
            req->set_fptr  (fp);
            req->set_ftype (openarchive::iopx_req::STAT_FOP);
            req->set_pos   (ptr);

            return;
        }

        void init_fstat_req (file_ptr_t fp, req_ptr_t req, struct stat *ptr)
        {
            req->set_fptr  (fp);
            req->set_ftype (openarchive::iopx_req::FSTAT_FOP);
            req->set_pos   (ptr);

            return;
        }

        void init_open_req (file_ptr_t fp, req_ptr_t req, uint64_t flags)
        {
            req->set_fptr  (fp);
            req->set_ftype (openarchive::iopx_req::OPEN_FOP);
            req->set_flags (flags);

            return;
        }
 
        void init_creat_req (file_ptr_t fp, req_ptr_t req, uint64_t flags, 
                             uint64_t mode)
        {
            req->set_fptr  (fp);
            req->set_ftype (openarchive::iopx_req::OPEN_FOP);
            req->set_flags (flags|O_CREAT);
            req->set_len   (mode);

            return;
        }

        void init_close_req (file_ptr_t fp, req_ptr_t req)
        {
            req->set_fptr  (fp);
            req->set_ftype (openarchive::iopx_req::CLOSE_FOP);

            return;
        }

        void init_read_req  (file_ptr_t fp, req_ptr_t req, uint64_t offset,
                             uint64_t count, uint64_t flag, struct iovec * iov)
        {
            req->set_fptr   (fp);
            req->set_ftype  (openarchive::iopx_req::PREAD_FOP);
            req->set_offset (offset);
            req->set_len    (count);
            req->set_flags  (flag);
            req->set_poi    (iov);

            return;
        }       

        void init_read_req  (file_ptr_t fp, req_ptr_t req, uint64_t offset, 
                             uint64_t count, uint64_t flag , buff_ptr_t buffp)
        {
            req->set_fptr   (fp);
            req->set_ftype  (openarchive::iopx_req::PREAD_FOP);
            req->set_offset (offset);
            req->set_len    (count);
            req->set_flags  (flag);
            req->set_bufp   (buffp);

            return;
        }

        void init_read_req  (file_ptr_t fp, req_ptr_t req, uint64_t offset, 
                             uint64_t count, uint64_t flag , void * buffp)
        {
            req->set_fptr   (fp);
            req->set_ftype  (openarchive::iopx_req::PREAD_FOP);
            req->set_offset (offset);
            req->set_len    (count);
            req->set_flags  (flag);
            req->set_pod    (buffp);

            return;
        }

        void init_write_req (file_ptr_t fp, req_ptr_t req, uint64_t offset,
                             uint64_t count, uint64_t flag, struct iovec * iov)
        {
            req->set_fptr   (fp);
            req->set_ftype  (openarchive::iopx_req::PWRITE_FOP);
            req->set_offset (offset);
            req->set_len    (count);
            req->set_flags  (flag);
            req->set_poi    (iov);

            return;
        }       

        void init_write_req (file_ptr_t fp, req_ptr_t req, uint64_t offset, 
                             uint64_t count, uint64_t flag , buff_ptr_t buffp)
        {
            req->set_fptr   (fp);
            req->set_ftype  (openarchive::iopx_req::PWRITE_FOP);
            req->set_offset (offset);
            req->set_len    (count);
            req->set_flags  (flag);
            req->set_bufp   (buffp);

            return;
        }

        void init_setxattr_req (file_ptr_t fp, req_ptr_t req, std::string name,
                                struct iovec * val, uint64_t flags)
        {
            req->set_fptr   (fp);
            req->set_ftype  (openarchive::iopx_req::SETX_FOP);
            req->set_desc   (name);
            req->set_poi    (val);
            req->set_flags  (flags);
            req->set_len    (val->iov_len);

            return;
        }

        void init_fsetxattr_req (file_ptr_t fp, req_ptr_t req, std::string name,
                                 struct iovec * val, uint64_t flags)
        {
            req->set_fptr   (fp);
            req->set_ftype  (openarchive::iopx_req::FSETX_FOP);
            req->set_desc   (name);
            req->set_poi    (val);
            req->set_flags  (flags);
            req->set_len    (val->iov_len);

            return;
        }

        void init_removexattr_req (file_ptr_t fp, req_ptr_t req, 
                                   std::string name)
        {
            req->set_fptr   (fp);
            req->set_ftype  (openarchive::iopx_req::REMOVEX_FOP);
            req->set_desc   (name);

            return;
        }

        void init_fremovexattr_req (file_ptr_t fp, req_ptr_t req, 
                                    std::string name)
        {
            req->set_fptr   (fp);
            req->set_ftype  (openarchive::iopx_req::FREMOVEX_FOP);
            req->set_desc   (name);

            return;
        }

        void init_mkdir_req (file_ptr_t fp, req_ptr_t req, uint64_t mode) 
        {
            req->set_fptr   (fp);
            req->set_flags  (mode);

            return;
        }

        void init_ftruncate_req (file_ptr_t fp, req_ptr_t req, uint64_t len)
        {
            req->set_fptr   (fp);
            req->set_len    (len);

            return;
        } 

        void init_getuuid_req (file_ptr_t fp, req_ptr_t req, struct iovec *val)
        {
            req->set_fptr   (fp);
            req->set_poi    (val);
            req->set_len    (val->iov_len);

            return;
        }

        void init_getxattr_req (file_ptr_t fp, req_ptr_t req, std::string name,
                                struct iovec * val)
        {
            req->set_fptr   (fp);
            req->set_ftype  (openarchive::iopx_req::GETX_FOP);
            req->set_desc   (name);
            req->set_poi    (val);
            req->set_len    (val->iov_len);

            return;
        }

        void init_fgetxattr_req (file_ptr_t fp, req_ptr_t req, std::string name,
                                 struct iovec * val)
        {
            req->set_fptr   (fp);
            req->set_ftype  (openarchive::iopx_req::FGETX_FOP);
            req->set_desc   (name);
            req->set_poi    (val);
            req->set_len    (val->iov_len);

            return;
        }

        void init_resolve_req (file_ptr_t fp, req_ptr_t req, 
                               std::list <arch_loc_t> * loc_list,
                               uint64_t len)
        {
            req->set_fptr   (fp);
            req->set_ftype  (openarchive::iopx_req::RESOLVE_FOP);
            req->set_len    (len);
            req->set_ploc   (loc_list);

            return;
        }

        void init_gethosts_req (file_ptr_t fp, req_ptr_t req,
                                std::list <std::string> * host_list)
        {
            req->set_fptr   (fp);
            req->set_ftype  (openarchive::iopx_req::GETHOSTS_FOP);
            req->set_phosts (host_list);

            return;
        } 

        void init_scan_req (file_ptr_t fp, req_ptr_t req,
                            std::string * path, archstore_scan_type_t type)
        {
            req->set_fptr   (fp);
            req->set_ftype  (openarchive::iopx_req::SCAN_FOP);
            req->set_str    (path);
            req->set_flags  (type); 

            return;
        }

    } /* End of namespace iopx_req */
} /* End of namespace openarchive */
