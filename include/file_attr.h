/*
  Copyright (c) 2006-2011 Commvault systems, Inc. <http://www.commvault.com>.
  This file is part of OpenArchive.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef __FILE_ATTR_H__
#define __FILE_ATTR_H__

namespace openarchive
{
    namespace file_attr
    {
        class file_attr
        {
            std::string product;
            std::string store;
            uuid_t      uuid;
            uint64_t    file_size;
            uint64_t    blk_size;
            uint64_t    num_blocks;

            public:
            file_attr (void): product (""), store (""), file_size (0),
                              blk_size (0), num_blocks (0)
            {
                uuid_clear (uuid);
            }

            void set_product (std::string &prod) { product    = prod;    }
            void set_store   (std::string &st)   { store      = st;      }
            void set_uuid    (uuid_t ud)         { uuid_copy (uuid, ud); }
            void set_fsize   (uint64_t sz)       { file_size  = sz;      }
            void set_blksize (uint64_t sz)       { blk_size   = sz;      }
            void set_blocks  (uint64_t blk)      { num_blocks = blk;     } 

            std::string& get_product (void)      { return product;       }
            std::string& get_store   (void)      { return store;         }
            void         get_uuid    (uuid_t u)  { uuid_copy (u, uuid);  }
            uint64_t     get_fsize   (void)      { return file_size;     }
            uint64_t     get_blksize (void)      { return blk_size;      }
            uint64_t     get_blocks  (void)      { return num_blocks;    } 

        }; 
    }

    typedef openarchive::file_attr::file_attr file_attr_t;
    typedef boost::shared_ptr <file_attr_t>   file_attr_ptr_t; 
}

#endif /* End of __FILE_ATTR_H__ */
