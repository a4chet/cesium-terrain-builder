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
  * @file CTBMBTileSerializer.cpp
  * @brief This defines the `CTBMBTileSerializer` class
  */

#include <stdio.h>
#include <string.h>
#include <mutex>

#include "MbTilesDb.hpp"
#include "../deps/concat.hpp"
#include "cpl_vsi.h"
#include "CTBException.hpp"
#include "CTBMBTileSerializer.hpp"
#include "CTBFileOutputStream.hpp"
#include "CTBZOutputStream.hpp"

using namespace std;
using namespace ctb;

ctb::CTBMBTileSerializer::CTBMBTileSerializer(const std::string &outputDir, const std::string &datasetName, bool resume) :
  moutputDir(outputDir),
  mresume(resume) {
 
  dbPath = outputDir + datasetName + ".mbtiles";

  if (mresume) {
    mbTiles = unique_ptr<MbTilesDb>(new MbTilesDb(dbPath));
    mbTiles->loadRenderedTiles(renderedTiles);
  }
  else {
    VSIUnlink(dbPath.c_str());
    mbTiles = unique_ptr<MbTilesDb>(new MbTilesDb(dbPath));
  }
}

/**
 * @details
 * Returns if the specified Tile Coordinate should be serialized
 */
bool 
ctb::CTBMBTileSerializer::mustSerializeCoordinate(const ctb::TileCoordinate *coordinate) {
  if (!mresume)
    return true;

  bool rendered = checkIfAlreadyRendered(*coordinate);
  if (rendered) alreadyRendered++;

  return !rendered;
}

bool 
ctb::CTBMBTileSerializer::hasCoordinate(const ctb::TileCoordinate &coordinate) {
  bool rendered = checkIfAlreadyRendered(coordinate);
  if (!rendered) {
    rendered = mbTiles->tileExists(coordinate.zoom, coordinate.x, coordinate.y);
  }
  return rendered;
}

void ctb::CTBMBTileSerializer::saveMetadata(const std::stringstream & strm) {
  mbTiles->saveMetadata(strm);
}

void
ctb::CTBMBTileSerializer::recordValidPoint(const TileCoordinate& coord) {
  static std::mutex mutex;
  std::lock_guard<std::mutex> lock(mutex);

  //TilePoint point(coord.x, coord.y);
  //validPoints[coord.zoom].push_back(point);

  uint64_t z = coord.zoom;
  uint64_t x = coord.x;
  uint64_t y = coord.y;

  validTiles++;

  renderedTiles.insert((z << 58) | (x << 29) | y);
}

bool 
ctb::CTBMBTileSerializer::checkIfAlreadyRendered(const TileCoordinate& coord) {
  static std::mutex mutex;
  std::lock_guard<std::mutex> lock(mutex);

  uint64_t z = coord.zoom;
  uint64_t x = coord.x;
  uint64_t y = coord.y;

  auto it = renderedTiles.find((z << 58) | (x << 29) | y);
  bool alreadyRendered = it != renderedTiles.end();
  return alreadyRendered;
}

/**
 * @details
 * Serialize a TerrainTile to the gzip os
 */
bool
ctb::CTBMBTileSerializer::serializeTile(const ctb::TerrainTile *tile) {
  const TileCoordinate *coordinate = tile;
  static std::mutex mutex;
  std::lock_guard<std::mutex> lock(mutex);

  CTBZOutputStream stream;
  tile->writeFile(stream);

  mbTiles->writeTile(
    coordinate->zoom, coordinate->x, coordinate->y,
    stream.str().c_str(),
    stream.size());

  //recordValidPoint(*coordinate);
  return true;
}

/**
 * @details
 * Serialize a MeshTile to the gzip os
 */
bool
ctb::CTBMBTileSerializer::serializeTile(const ctb::MeshTile *tile, bool writeVertexNormals) {
  
  const TileCoordinate *coordinate = tile;
  static std::mutex mutex;
  std::lock_guard<std::mutex> lock(mutex);

  CTBZOutputStream stream;
  tile->writeFile(stream, writeVertexNormals);

  mbTiles->writeTile(
    coordinate->zoom, coordinate->x, coordinate->y, 
    stream.str().c_str(),
    stream.size());

  //recordValidPoint(*coordinate);
  return true;
}
