/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __ARCH_LOC_H__
#define __ARCH_LOC_H__

#include <uuid/uuid.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/convenience.hpp>

namespace openarchive
{
    namespace arch_loc
    {
        class arch_loc
        {
            std::string product_id;
            std::string store_id;
            boost::filesystem::path path;
            boost::uuids::uuid uuid;
            std::string uuid_str;

            private:
            std::string uuid_to_str (void)
            {
                return (boost::uuids::to_string(uuid));
            }

            public:
            arch_loc(void): product_id(""),store_id(""),path("")
            {
                boost::uuids::nil_generator nil_gen;
                uuid = nil_gen();
                uuid_str = uuid_to_str ();
            }

            arch_loc(std::string &prod, std::string &stor,std::string &pth, 
                     uuid_t &uid): product_id(prod),store_id(stor), path(pth)
            {
                memcpy (&uuid, uid, uuid.size());
                uuid_str = uuid_to_str ();
            }

            arch_loc(std::string &prod, std::string &stor,std::string &pth):
                     product_id(prod),store_id(stor), path(pth)
            {
                boost::uuids::nil_generator nil_gen;
                uuid = nil_gen();
                uuid_str = uuid_to_str ();
            }

            arch_loc(std::string &pth, uuid_t &uid): product_id(""),store_id(""),
                                                   path(pth)
            {
                memcpy (&uuid, uid, uuid.size());
                uuid_str = uuid_to_str ();
            }

            void set_product (std::string &pr)
            {
                product_id = pr;
            }
               
            void set_store (std::string &st)
            {
                store_id = st;
            }

            void set_path (std::string &p) 
            {
                path = p;
            }

            void set_path (const std::string &p) 
            {
                path = p;
            }

            void set_uuid (uuid_t uid)
            {
                memcpy (&uuid, uid, uuid.size());
                uuid_str = uuid_to_str ();
            }

            std::string& get_product (void)
            {
                return product_id;
            }
 
            std::string& get_store (void)
            {
                return store_id;
            }

            const std::string& get_pathstr (void)
            {
                return (path.string());
            }

            void get_uuid (uuid_t uid)
            {
                memcpy (uid, &uuid, uuid.size());
            }

            uuid_t * get_uuid_ptr (void)
            {
                return (uuid_t *) uuid.data;
            }

            std::string& get_uuidstr (void)
            {
                return uuid_str;
            } 
             
        };
    }

    typedef openarchive::arch_loc::arch_loc arch_loc_t;
    typedef boost::shared_ptr <arch_loc_t>  arch_loc_ptr_t;
}

#endif /* End of __ARCH_LOC_H__ */
