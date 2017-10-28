/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __LOGGER_H__
#define __LOGGER_H__

#include <stdexcept>
#include <string>
#include <execinfo.h>
#include <boost/shared_ptr.hpp>
#include <boost/lambda/lambda.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/log/common.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/attributes.hpp>
#include <boost/log/sinks.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/attributes/current_process_id.hpp>
#include <boost/log/attributes/attribute_value_set.hpp>

namespace logging = boost::log;
namespace attrs = boost::log::attributes;
namespace src = boost::log::sources;
namespace sinks = boost::log::sinks;
namespace expr = boost::log::expressions;
namespace keywords = boost::log::keywords;
namespace mt_attrs = boost::log::attributes;


namespace openarchive
{
    namespace logger
    {
        typedef sinks::synchronous_sink<sinks::text_file_backend>\
                file_sink;
        typedef boost::shared_ptr<file_sink> file_sink_ptr;

        /*
         * Constants defining different log levels
         */
        typedef enum 
        {
            level_error=0,
            level_warn=1,
            level_debug_2=2,
            level_debug_3=3,
            level_debug_4=4,
            level_debug_5=5
        } loglevel_t;
            

        class logger
        {
            file_sink_ptr sink;

            public:
            logger (std::string, std::string, uint, uint);
            ~logger (void);
            void set_log_rotation (std::string, uint, uint);
            void init_logger (std::string, std::string, uint, uint);
            void flush (void);
        };
        void get_backtrace (std::string &);
    } /* namespace cfgparams */  
} /* namespace openarchive */

#endif /* End of __LOGGER_H__ */
