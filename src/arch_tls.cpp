/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <arch_tls.h>

namespace openarchive
{
    namespace arch_tls
    {
        void arch_tls::log_statistics (void)
        {
            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " File descriptor memory pool statistics "
                               << " allocated: " <<  file_pool.get_alloced ()
                               << " freed: " << file_pool.get_freed ()
                               << " next size: " 
                               << file_pool.get_next_req_size ();
            } 

            {
                BOOST_LOG_FUNCTION ();
                BOOST_LOG_SEV (log, openarchive::logger::level_error)
                               << " Request info memory pool statistics    "
                               << " allocated: " <<  req_pool.get_alloced ()
                               << " freed: " << req_pool.get_freed ()
                               << " next size: " 
                               << req_pool.get_next_req_size ();
            }

            return;
        }

        boost::thread_specific_ptr<arch_tls> &  get_arch_tls (void)
        {
            /*
             * Allocate thread local storage for the thread, if it 
             * isn't allocated currently. All the future memory requests
             * will be serviced from this tls.
             */

            static boost::thread_specific_ptr<arch_tls> tls;

            if (!tls.get()) {

                tls.reset(new arch_tls);
                tls->set_stream_ptr (NULL); 

            }

            return tls;
          
        } 

    }
} 
