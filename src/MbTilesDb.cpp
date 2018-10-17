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
  * @file MbTilesDb.cpp
  * @brief This defines the `MbTilesDb` class
  */

#include <mutex>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <stdexcept>
#include <sstream>

#include "MbTilesDb.hpp"

ctb::MbTilesDb::MbTilesDb(std::string const& dbname) {

  sqlite3 *db;
  if (sqlite3_open(dbname.c_str(), &db) != SQLITE_OK) {
    std::ostringstream err;
    err << "SQLite Error: Failed to open " << dbname << " - " << sqlite3_errmsg(db) << std::endl;
    throw std::runtime_error(err.str());
  }

  mbTiles = db;
  char *err_msg = NULL;
  if (sqlite3_exec(mbTiles, "PRAGMA synchronous=0", NULL, NULL, &err_msg) != SQLITE_OK) {
    std::ostringstream err;
    err << "SQLite Error: Async error: " << err_msg << std::endl;
    throw std::runtime_error(err.str());
  }
  if (sqlite3_exec(mbTiles, "PRAGMA locking_mode=EXCLUSIVE", NULL, NULL, &err_msg) != SQLITE_OK) {
    std::ostringstream err;
    err << "SQLite Error: Async error: " << err_msg << std::endl;
    throw std::runtime_error(err.str());
  }
  if (sqlite3_exec(mbTiles, "PRAGMA journal_mode=DELETE", NULL, NULL, &err_msg) != SQLITE_OK) {
    std::ostringstream err;
    err << "SQLite Error: Async error: " << err_msg << std::endl;
    throw std::runtime_error(err.str());
  }
  if (sqlite3_exec(mbTiles, "CREATE TABLE IF NOT EXISTS metadata (name text, value text);", NULL, NULL, &err_msg) != SQLITE_OK) {
    std::ostringstream err;
    err << "SQLite Error: Metadata Table Creation error: " << err_msg << std::endl;
    throw std::runtime_error(err.str());
  }
  if (sqlite3_exec(mbTiles, "CREATE TABLE IF NOT EXISTS  tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);", NULL, NULL, &err_msg) != SQLITE_OK) {
    std::ostringstream err;
    err << "SQLite Error: Tiles Table Creation error: " << err_msg << std::endl;
    throw std::runtime_error(err.str());
  }
  if (sqlite3_exec(mbTiles, "create unique index IF NOT EXISTS  name on metadata (name);", NULL, NULL, &err_msg) != SQLITE_OK) {
    std::ostringstream err;
    err << "SQLite Error: Metadata Index Creation error: " << err_msg << std::endl;
    throw std::runtime_error(err.str());
  }
  if (sqlite3_exec(mbTiles, "create unique index IF NOT EXISTS  tile_index on tiles (zoom_level, tile_column, tile_row);", NULL, NULL, &err_msg) != SQLITE_OK) {
    std::ostringstream err;
    err << "SQLite Error: Tiles Index Creation error: " << err_msg << std::endl;
    throw std::runtime_error(err.str());
  }

  // Construct tile insertion prepared statement
  sqlite3_stmt *stmt;
  const char *query = "insert into tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?)";
  if (sqlite3_prepare_v2(mbTiles, query, -1, &stmt, NULL) != SQLITE_OK) {
    std::ostringstream err;
    err << "SQLite Error: Tile prepared statement failed to create." << std::endl;
    throw std::runtime_error(err.str());
  }
  tile_stmt = stmt;
}

ctb::MbTilesDb::~MbTilesDb() {
  char *err;
  std::ostringstream err_msg;

  if (sqlite3_exec(mbTiles, "ANALYZE;", NULL, NULL, &err) != SQLITE_OK) {
    err_msg << "SQLite Error: failed to ANALYZE: " << err << std::endl;
    throw std::runtime_error(err_msg.str());
  }
  if (sqlite3_finalize(tile_stmt) != SQLITE_OK) {
    err_msg << "SQLite Error: failed to finalize tile_stmt " << std::endl;
    throw std::runtime_error(err_msg.str());
  }
  if (sqlite3_close(mbTiles) != SQLITE_OK) {
    err_msg << "SQLite Error: failed to close sqlite3 db" << std::endl;
    throw std::runtime_error(err_msg.str());
  }
}

void
ctb::MbTilesDb::loadRenderedTiles(std::unordered_set<uint64_t>& renderedTiles) {
  sqlite3_stmt *stmt;
  if (sqlite3_prepare_v2(mbTiles, "SELECT zoom_level, tile_column, tile_row FROM tiles;", -1, &stmt, nullptr) != SQLITE_OK) {
    std::string err = "Could not prepare tile fetching statement";
    throw std::runtime_error(err);
  }

  int rc;
  while (true) {
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
      uint64_t z = sqlite3_column_int(stmt, 0);
      uint64_t x = sqlite3_column_int(stmt, 1);
      uint64_t y = sqlite3_column_int(stmt, 2);
      renderedTiles.insert((z << 58) | (x << 29) | y);
    }
    else if (rc == SQLITE_DONE) {
      break;
    }
    else {
      std::string err = "Could not fetch rendered rows from database";
      throw std::runtime_error(err);
    }
  }
  sqlite3_finalize(stmt);
}

void ctb::MbTilesDb::writeTile(int z, int x, int y, const char *data, int size) {
  sqlite3_stmt *stmt = tile_stmt;
  sqlite3_reset(stmt);
  sqlite3_clear_bindings(stmt);
  sqlite3_bind_int(stmt, 1, z);
  sqlite3_bind_int(stmt, 2, x);
  //sqlite3_bind_int(stmt, 3, (1 << z) - 1 - y);
  sqlite3_bind_int(stmt, 3, y);
  sqlite3_bind_blob(stmt, 4, data, size, NULL);
  if (sqlite3_step(stmt) != SQLITE_DONE) {
    std::cerr << "SQLite Error: tile insert failed: " << sqlite3_errmsg(mbTiles) << std::endl;
  }
}

void ctb::MbTilesDb::quote(std::ostringstream & buf, std::string const& input) {
  for (auto & ch : input) {
    if (ch == '\\' || ch == '\"') {
      buf << '\\';
      buf << ch;
    }
    else if (ch < ' ') {
      char tmp[7];
      sprintf(tmp, "\\u%04x", ch);
      buf << tmp;
    }
    else {
      buf << ch;
    }
  }
}

void ctb::MbTilesDb::saveMetadata(const std::stringstream & strm){
  char *sql, *err;

  if (sqlite3_exec(mbTiles, "DELETE FROM metadata WHERE name = 'layer_json'", NULL, NULL, &err) != SQLITE_OK) {
    std::ostringstream err_msg;
    err_msg << "SQLite Error: failed to set metadata: " << err << std::endl;
    throw std::runtime_error(err_msg.str());
  }

  sql = sqlite3_mprintf("INSERT INTO metadata (name, value) VALUES ('layer_json', %Q);", strm.str().c_str());
  if (sqlite3_exec(mbTiles, sql, NULL, NULL, &err) != SQLITE_OK) {
    sqlite3_free(sql);
    std::ostringstream err_msg;
    err_msg << "SQLite Error: failed to set metadata: " << err << std::endl;
    throw std::runtime_error(err_msg.str());
  }
  sqlite3_free(sql);
}

bool ctb::MbTilesDb::tileExists(ctb::i_zoom z, ctb::i_tile x, ctb::i_tile y) {

  char *sql, *err;

  sql = sqlite3_mprintf("SELECT zoom_level, tile_column, tile_row FROM tiles WHERE zoom_level = %u AND tile_column = %u AND tile_row = %u;", z, x, y);

  sqlite3_stmt *stmt;
  if (sqlite3_prepare_v2(mbTiles, sql, -1, &stmt, nullptr) != SQLITE_OK) {
    sqlite3_free(sql);
    std::ostringstream err_msg;
    err_msg << "SQLite Error: failed to prepare stmt for query tileExists: " << err << std::endl;
    throw std::runtime_error(err_msg.str());
  }
  sqlite3_free(sql);
  
  int rc = sqlite3_step(stmt);
  bool exists = (rc == SQLITE_ROW);  

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    std::string err = "Could not query tileExists";
    throw std::runtime_error(err);
  }
  sqlite3_finalize(stmt);
  return exists;
}

#if 0
void ctb::MbTilesDb::writeMetadata(
  std::string const& fname,
  int minzoom,
  int maxzoom,
  layer_map_type const &layermap) {
  char *sql, *err;

  sql = sqlite3_mprintf("INSERT INTO metadata (name, value) VALUES ('name', %Q);", fname.c_str());
  if (sqlite3_exec(mbTiles, sql, NULL, NULL, &err) != SQLITE_OK) {
    sqlite3_free(sql);
    std::ostringstream err_msg;
    err_msg << "SQLite Error: failed to set name in metadata: " << err << std::endl;
    throw std::runtime_error(err_msg.str());
  }
  sqlite3_free(sql);

  sql = sqlite3_mprintf("INSERT INTO metadata (name, value) VALUES ('description', %Q);", fname.c_str());
  if (sqlite3_exec(mbTiles, sql, NULL, NULL, &err) != SQLITE_OK) {
    sqlite3_free(sql);
    std::ostringstream err_msg;
    err_msg << "SQLite Error: failed to set description in metadata: " << err << std::endl;
    throw std::runtime_error(err_msg.str());
  }
  sqlite3_free(sql);

  sql = sqlite3_mprintf("INSERT INTO metadata (name, value) VALUES ('version', %d);", 2);
  if (sqlite3_exec(mbTiles, sql, NULL, NULL, &err) != SQLITE_OK) {
    sqlite3_free(sql);
    std::ostringstream err_msg;
    err_msg << "SQLite Error: failed to set version in metadata: " << err << std::endl;
    throw std::runtime_error(err_msg.str());
  }
  sqlite3_free(sql);

  sql = sqlite3_mprintf("INSERT INTO metadata (name, value) VALUES ('minzoom', %d);", minzoom);
  if (sqlite3_exec(mbTiles, sql, NULL, NULL, &err) != SQLITE_OK) {
    sqlite3_free(sql);
    std::ostringstream err_msg;
    err_msg << "SQLite Error: failed to set minzoom in metadata: " << err << std::endl;
    throw std::runtime_error(err_msg.str());
  }
  sqlite3_free(sql);

  sql = sqlite3_mprintf("INSERT INTO metadata (name, value) VALUES ('maxzoom', %d);", maxzoom);
  if (sqlite3_exec(mbTiles, sql, NULL, NULL, &err) != SQLITE_OK) {
    sqlite3_free(sql);
    std::ostringstream err_msg;
    err_msg << "SQLite Error: failed to set maxzoom in metadata: " << err << std::endl;
    throw std::runtime_error(err_msg.str());
  }
  sqlite3_free(sql);

  sql = sqlite3_mprintf("INSERT INTO metadata (name, value) VALUES ('center', '0.0,0.0,%d');", maxzoom);
  if (sqlite3_exec(mbTiles, sql, NULL, NULL, &err) != SQLITE_OK) {
    sqlite3_free(sql);
    std::ostringstream err_msg;
    err_msg << "SQLite Error: failed to set center in metadata: " << err << std::endl;
    throw std::runtime_error(err_msg.str());
  }
  sqlite3_free(sql);

  double minlon = -180;
  double minlat = -85.05112877980659;
  double maxlon = 180;
  double maxlat = 85.0511287798066;
  sql = sqlite3_mprintf("INSERT INTO metadata (name, value) VALUES ('bounds', '%f,%f,%f,%f');", minlon, minlat, maxlon, maxlat);
  if (sqlite3_exec(mbTiles, sql, NULL, NULL, &err) != SQLITE_OK) {
    sqlite3_free(sql);
    std::ostringstream err_msg;
    err_msg << "SQLite Error: failed to set bounds in metadata: " << err << std::endl;
    throw std::runtime_error(err_msg.str());
  }
  sqlite3_free(sql);

  sql = sqlite3_mprintf("INSERT INTO metadata (name, value) VALUES ('type', %Q);", "overlay");
  if (sqlite3_exec(mbTiles, sql, NULL, NULL, &err) != SQLITE_OK) {
    sqlite3_free(sql);
    std::ostringstream err_msg;
    err_msg << "SQLite Error: failed to set type in metadata: " << err << std::endl;
    throw std::runtime_error(err_msg.str());
  }
  sqlite3_free(sql);

  sql = sqlite3_mprintf("INSERT INTO metadata (name, value) VALUES ('format', %Q);", "pbf");
  if (sqlite3_exec(mbTiles, sql, NULL, NULL, &err) != SQLITE_OK) {
    sqlite3_free(sql);
    std::ostringstream err_msg;
    err_msg << "SQLite Error: failed to set format in metadata: " << err << std::endl;
    throw std::runtime_error(err_msg.str());
  }
  sqlite3_free(sql);

  std::ostringstream buf;
  buf << "{\"vector_layers\": [ ";

  bool first = true;
  for (auto const& ai : layermap) {
    if (first) {
      first = false;
      buf << "{ \"id\": \"";
    }
    else {
      buf << ", { \"id\": \"";
    }
    quote(buf, ai.first);
    buf << "\", \"description\": \"\", \"minzoom\": ";
    buf << ai.second.min_zoom;
    buf << ", \"maxzoom\": ";
    buf << ai.second.max_zoom;
    buf << ", \"fields\": {";
    bool first_field = true;
    for (auto const& j : ai.second.fields) {
      if (first_field) {
        first_field = false;
        buf << "\"";
      }
      else {
        buf << ", \"";
      }
      quote(buf, j.first);
      if (j.second == json_field_type_number) {
        buf << "\": \"Number\"";
      }
      else if (j.second == json_field_type_boolean) {
        buf << "\": \"Boolean\"";
      }
      else {
        buf << "\": \"String\"";
      }
    }
    buf << "} }";
  }
  buf << " ] }";

  sql = sqlite3_mprintf("INSERT INTO metadata (name, value) VALUES ('json', %Q);", buf.str().c_str());
  if (sqlite3_exec(mbTiles, sql, NULL, NULL, &err) != SQLITE_OK) {
    sqlite3_free(sql);
    std::ostringstream err_msg;
    err_msg << "SQLite Error: failed to set json in metadata: " << err << std::endl;
    throw std::runtime_error(err_msg.str());
  }
  sqlite3_free(sql);
}
#endif

