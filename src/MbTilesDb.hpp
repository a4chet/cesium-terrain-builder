#ifndef MBTILESDB_HPP
#define MBTILESDB_HPP

/*******************************************************************************
 * Copyright 2018 GeoData <geodata@soton.ac.uk>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/

 /**
  * @file MBTilesDB.hpp
  * @brief This declares and defines the `MBTilesDB` class
  */

#include <string>
#include <unordered_set>
#include <map>

#include "types.hpp"
#include "sqlite3.h"
#include "config.hpp"

enum json_field_type : std::uint8_t {
  json_field_type_number = 0,
  json_field_type_boolean,
  json_field_type_string
};

struct layer_meta_data {
  int min_zoom;
  int max_zoom;
  std::map<std::string, json_field_type> fields;
};

using layer_map_type = std::map<std::string, layer_meta_data>;

namespace ctb {
  class MbTilesDb;
}

class CTB_DLL ctb::MbTilesDb {
public:
  MbTilesDb(std::string const& dbname);
  virtual ~MbTilesDb();

  void loadRenderedTiles(std::unordered_set<uint64_t>& renderedTiles);

  void writeTile(int z, int x, int y, const char *data, int size);

  void quote(std::ostringstream & buf, std::string const& input);

  void saveMetadata(const std::stringstream & strm);

  bool tileExists(ctb::i_zoom z, ctb::i_tile x, ctb::i_tile y);

/*
  void writeMetadata(
    std::string const& fname,
    int minzoom,
    int maxzoom,
    layer_map_type const &layermap);
*/
protected:
  sqlite3* mbTiles;
  sqlite3_stmt* tile_stmt;
};


#endif /* MBTILESDB_HPP */