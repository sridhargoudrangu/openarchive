/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <arch_iopx.h>

namespace openarchive
{
    namespace arch_iopx
    {
        arch_iopx::arch_iopx (std::string n, io_service_ptr_t is): name(n), 
                                                                   iosvc(is)
        {
            /*
             * For the base iopx we don't have nothing much to do. But the 
             * other one's might check for the existence of a parent and 
             * number of children and other stuff.
             */ 

            refcount.store (0); /* Initialize the ref count to zero */

        }

        arch_iopx::~arch_iopx (void)
        {
            while (get_refcount () > 0 ) {
                /*
                 * Wait until there are no more active references
                 */ 
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        } 

        void arch_iopx::put (void)
        {
            refcount.fetch_sub (1);
        }
            
        void arch_iopx::get (void)
        {
            refcount.fetch_add (1);
        }
            
        uint64_t arch_iopx::get_refcount (void)
        {
            return (atomic_load (&refcount));
        }

        std::error_code arch_iopx::run_fop (file_ptr_t fp, req_ptr_t req)
        {
            switch (req->get_ftype())
            {
                case openarchive::iopx_req::OPEN_FOP:
                    return open (fp, req);
                    break;

                case openarchive::iopx_req::CLOSE_FOP:
                    return close (fp, req);
                    break;

                case openarchive::iopx_req::PREAD_FOP:
                    return pread (fp, req);
                    break;

                case openarchive::iopx_req::PWRITE_FOP:
                    return pwrite (fp, req);
                    break;

                case openarchive::iopx_req::FSTAT_FOP:
                    return fstat (fp, req);
                    break;

                case openarchive::iopx_req::STAT_FOP:
                    return stat (fp, req);
                    break;

                case openarchive::iopx_req::FTRUNCATE_FOP:
                    return ftruncate (fp, req);
                    break;

                case openarchive::iopx_req::TRUNCATE_FOP:
                    return truncate (fp, req);
                    break;

                case openarchive::iopx_req::FSETX_FOP:
                    return fsetxattr (fp, req);
                    break;

                case openarchive::iopx_req::SETX_FOP:
                    return setxattr (fp, req);
                    break;

                case openarchive::iopx_req::FGETX_FOP:
                    return fgetxattr (fp, req);
                    break;

                case openarchive::iopx_req::GETX_FOP:
                    return getxattr (fp, req);
                    break;

                case openarchive::iopx_req::FREMOVEX_FOP:
                    return fremovexattr (fp, req);
                    break;

                case openarchive::iopx_req::REMOVEX_FOP:
                    return removexattr (fp, req);
                    break;

                case openarchive::iopx_req::LSEEK_FOP:
                    return lseek (fp, req);
                    break;

                case openarchive::iopx_req::MKDIR_FOP:
                    return mkdir (fp, req);
                    break;

                case openarchive::iopx_req::GETUUID_FOP:
                    return getuuid (fp, req);
                    break;

                case openarchive::iopx_req::RESOLVE_FOP:
                    return resolve (fp, req);
                    break;

                case openarchive::iopx_req::GETHOSTS_FOP:
                    return gethosts (fp, req);
                    break;

                case openarchive::iopx_req::SCAN_FOP:
                    return scan (fp, req);
                    break;

                default:
                    std::error_code ec (EINVAL, std::generic_category ());
                    return ec;
                    break;

            }
        }

        bool arch_iopx::schedule_fop (req_ptr_t req)
        {
            switch (req->get_ftype())
            {
                case openarchive::iopx_req::OPEN_FOP:
                    return schedule_open ();
                    break;

                case openarchive::iopx_req::CLOSE_FOP:
                    return schedule_close ();
                    break;

                case openarchive::iopx_req::PREAD_FOP:
                    return schedule_pread ();
                    break;

                case openarchive::iopx_req::PWRITE_FOP:
                    return schedule_pwrite ();
                    break;

                case openarchive::iopx_req::FSTAT_FOP:
                    return schedule_fstat ();
                    break;

                case openarchive::iopx_req::STAT_FOP:
                    return schedule_stat ();
                    break;

                case openarchive::iopx_req::FTRUNCATE_FOP:
                    return schedule_ftruncate ();
                    break;

                case openarchive::iopx_req::TRUNCATE_FOP:
                    return schedule_truncate ();
                    break;

                case openarchive::iopx_req::FSETX_FOP:
                    return schedule_fsetxattr ();
                    break;

                case openarchive::iopx_req::SETX_FOP:
                    return schedule_setxattr ();
                    break;

                case openarchive::iopx_req::FGETX_FOP:
                    return schedule_fgetxattr ();
                    break;

                case openarchive::iopx_req::GETX_FOP:
                    return schedule_getxattr ();
                    break;

                case openarchive::iopx_req::FREMOVEX_FOP:
                    return schedule_fremovexattr ();
                    break;

                case openarchive::iopx_req::REMOVEX_FOP:
                    return schedule_removexattr ();
                    break;

                case openarchive::iopx_req::LSEEK_FOP:
                    return schedule_lseek ();
                    break;

                case openarchive::iopx_req::MKDIR_FOP:
                    return schedule_mkdir ();
                    break;

                case openarchive::iopx_req::GETUUID_FOP:
                    return schedule_getuuid ();
                    break;

                case openarchive::iopx_req::RESOLVE_FOP:
                    return schedule_resolve ();
                    break;

                case openarchive::iopx_req::GETHOSTS_FOP:
                    return schedule_gethosts ();
                    break;

                case openarchive::iopx_req::SCAN_FOP:
                    return schedule_scan ();
                    break;

                default:
                    return false;    
                    break;

            }
        }

        std::error_code arch_iopx::parent_cbk (file_ptr_t fp, req_ptr_t req,
                                               std::error_code ec)
        {
            /*
             * Check whether this is the root iopx. In that case there 
             * will be no parent iopx to be invoked. The callback would be
             * invoked.
             */
            if (!parent) {
                /*
                 * root iopx
                 */
            }

            switch (req->get_ftype())
            {
                case openarchive::iopx_req::OPEN_FOP:
                    return parent->open_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::CLOSE_FOP:
                    return parent->close_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::PREAD_FOP:
                    return parent->pread_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::PWRITE_FOP:
                    return parent->pwrite_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::FSTAT_FOP:
                    return parent->fstat_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::STAT_FOP:
                    return parent->stat_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::FTRUNCATE_FOP:
                    return parent->ftruncate_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::TRUNCATE_FOP:
                    return parent->truncate_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::FSETX_FOP:
                    return parent->fsetxattr_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::SETX_FOP:
                    return parent->setxattr_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::FGETX_FOP:
                    return parent->fgetxattr_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::GETX_FOP:
                    return parent->getxattr_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::FREMOVEX_FOP:
                    return parent->fremovexattr_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::REMOVEX_FOP:
                    return parent->removexattr_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::LSEEK_FOP:
                    return parent->lseek_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::MKDIR_FOP:
                    return parent->mkdir_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::GETUUID_FOP:
                    return parent->getuuid_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::RESOLVE_FOP:
                    return parent->resolve_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::GETHOSTS_FOP:
                    return parent->gethosts_cbk (fp, req, ec);
                    break;

                case openarchive::iopx_req::SCAN_FOP:
                    return parent->scan_cbk (fp, req, ec);
                    break;

                default:
                    std::error_code ec (EINVAL, std::generic_category ());
                    return ec;
                    break;

            }
        }

        /*
         * Default fop implementation
         */
        std::error_code arch_iopx::fop_default (file_ptr_t fp, req_ptr_t rq)
        {
            std::list<boost::shared_ptr<arch_iopx>>::iterator iter;

            /*
             * Update the number of children that this iopx has.
             */
            if (false == rq->set_childcount (name, children.size())) {
                std::error_code ec (EPERM, std::generic_category ());
                return (ec);
            }

            for(iter = children.begin(); iter != children.end(); iter++) {
                /*
                 * Start executing each of the child iopx.
                 */   
                if (false == (*iter)->schedule_fop (rq)) {
                    /*
                     * Invoke the child iopx directly.
                     */
                    std::error_code ec = (*iter)->run_fop (fp, rq);
                    if (ec != openarchive::ok) {
                        return (ec);
                    } 
                } else {
                    /*
                     * The child needs to be executed in the conext of a 
                     * separate thread.
                     */
                    iosvc->post(boost::bind(&arch_iopx::run_fop, *iter, fp,rq));
                }
            }

            return (openarchive::success);
        }

        /*
         * Default close implementation
         */
        std::error_code arch_iopx::close_default (file_t & fp)
        {
            std::list<boost::shared_ptr<arch_iopx>>::iterator iter;

            for(iter = children.begin(); iter != children.end(); iter++) {
                /*
                 * Start executing each of the child iopx directly.
                 */
                  
                std::error_code ec = (*iter)->close (fp);
                if (ec != openarchive::ok) {
                        return (ec);
                }
            }

            return (openarchive::success);
        }

        /*
         * Default dup implementation
         */
        std::error_code arch_iopx::dup_default (file_ptr_t src_fp, 
                                                file_ptr_t dest_fp)
        {
            std::list<boost::shared_ptr<arch_iopx>>::iterator iter;

            for(iter = children.begin(); iter != children.end(); iter++) {
                /*
                 * Start executing each of the child iopx directly.
                 */   
                std::error_code ec = (*iter)->dup (src_fp, dest_fp);
                if (ec != openarchive::ok) {
                    return (ec);
                }
            }

            return (openarchive::success);
        }

        /*
         * Default fop callback implementation
         */
        std::error_code arch_iopx::fop_cbk_default (file_ptr_t fp, req_ptr_t rq,
                                                    std::error_code ec) 
        {
            /*
             * Increase the response count.
             */
            if (false == rq->post_resp (name)) {

                std::error_code ecode (ENOKEY, std::generic_category ());
                parent_cbk (fp, rq, ecode);
                return (ecode);
            }

            if (ec != openarchive::ok) {

                /*
                 * Invoke call back of the parent.
                 */
                parent_cbk (fp, rq, ec);

            } else {

                /*
                 * Check whether response has been received for all children.
                 */
    
                uint64_t child, resp;
                if (rq->get_childcount (name, child) && 
                    rq->get_resp (name, resp) && 
                    child==resp) {
                    parent_cbk (fp, rq, ec);
                }
            } 

            return (openarchive::success);
        }

        /*
         * File operations
         */

        std::error_code arch_iopx::open (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::close (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::close (file_t & fp)
        {
            return close_default (fp);
        }

        std::error_code arch_iopx::pread (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::pwrite (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::fstat (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::stat (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::ftruncate (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::truncate (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::fsetxattr (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::setxattr (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::fgetxattr (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::getxattr (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::fremovexattr (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::removexattr (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::lseek (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::getuuid (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::gethosts (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        /*
         * File system operations
         */

        std::error_code arch_iopx::mkdir (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::resolve (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq);
        }

        std::error_code arch_iopx::dup (file_ptr_t src_fp, file_ptr_t dest_fp)
        {
            return dup_default (src_fp, dest_fp); 
        }   

        std::error_code arch_iopx::scan (file_ptr_t fp, req_ptr_t rq)
        {
            return fop_default (fp, rq); 
        }   

        /*
         * File operation callbacks
         */

        std::error_code arch_iopx::open_cbk (file_ptr_t fp, req_ptr_t rq,
                                             std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        std::error_code arch_iopx::close_cbk (file_ptr_t fp, req_ptr_t rq,
                                              std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        std::error_code arch_iopx::pread_cbk (file_ptr_t fp, req_ptr_t rq,
                                              std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        std::error_code arch_iopx::pwrite_cbk (file_ptr_t fp, req_ptr_t rq,
                                               std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        std::error_code arch_iopx::fstat_cbk (file_ptr_t fp, req_ptr_t rq,
                                              std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        std::error_code arch_iopx::stat_cbk (file_ptr_t fp, req_ptr_t rq,
                                             std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        std::error_code arch_iopx::ftruncate_cbk (file_ptr_t fp, req_ptr_t rq,
                                                  std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        std::error_code arch_iopx::truncate_cbk (file_ptr_t fp, req_ptr_t rq,
                                                 std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        std::error_code arch_iopx::fsetxattr_cbk (file_ptr_t fp, req_ptr_t rq,
                                                  std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        std::error_code arch_iopx::setxattr_cbk (file_ptr_t fp, req_ptr_t rq,
                                                 std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        std::error_code arch_iopx::fgetxattr_cbk (file_ptr_t fp, req_ptr_t rq,
                                                  std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        std::error_code arch_iopx::getxattr_cbk (file_ptr_t fp, req_ptr_t rq,
                                                 std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        std::error_code arch_iopx::fremovexattr_cbk (file_ptr_t fp, req_ptr_t q,
                                                     std::error_code ec)
        {
            return fop_cbk_default (fp, q, ec);
        }

        std::error_code arch_iopx::removexattr_cbk (file_ptr_t fp, req_ptr_t rq,
                                                    std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        std::error_code arch_iopx::lseek_cbk (file_ptr_t fp, req_ptr_t rq,
                                              std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        std::error_code arch_iopx::getuuid_cbk (file_ptr_t fp, req_ptr_t rq,
                                                std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        std::error_code arch_iopx::gethosts_cbk (file_ptr_t fp, req_ptr_t rq,
                                                 std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        /*
         * File system operation callbacks
         */

        std::error_code arch_iopx::mkdir_cbk (file_ptr_t fp, req_ptr_t rq,
                                              std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        std::error_code arch_iopx::resolve_cbk (file_ptr_t fp, req_ptr_t rq,
                                                std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        std::error_code arch_iopx::scan_cbk (file_ptr_t fp, req_ptr_t rq,
                                             std::error_code ec)
        {
            return fop_cbk_default (fp, rq, ec);
        }

        /*
         * Profiling functions
         */
        void arch_iopx::profile (void)
        {
            std::list<boost::shared_ptr<arch_iopx>>::iterator iter;

            for(iter = children.begin(); iter != children.end(); iter++) {

                /*
                 * Start executing each of the child iopx.
                 */   
                (*iter)->profile ();
            }

            return;
        }                 

        /*
         * Reset the parent/child links
         */
        void arch_iopx::reset_links (void)
        {
            std::list<boost::shared_ptr<arch_iopx>>::iterator iter;

            for(iter = children.begin(); iter != children.end(); iter++) {
               
                if ((*iter)) {

                    (*iter)->reset_links ();

                }
            }

            iopx_ptr_t dummy;
            parent = dummy;

            return;
        }
  
    } /* End of namespace arch_iopx */
} /* End of namespace arch_iopx */
