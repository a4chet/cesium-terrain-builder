#ifndef CTBMBTILESERIALIZER_HPP
#define CTBMBTILESERIALIZER_HPP

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
  * @file CTBFileTileSerializer.hpp
  * @brief This declares and defines the `CTBFileTileSerializer` class
  */

#include <string>
#include <unordered_set>
#include <vector>

#include "MBTilesDb.hpp"
#include "TileCoordinate.hpp"
#include "GDALSerializer.hpp"
#include "TerrainSerializer.hpp"
#include "MeshSerializer.hpp"

namespace ctb {
	class CTBMBTileSerializer;
}

/// Implements a serializer of `Tile`s based in a directory of files
class CTB_DLL ctb::CTBMBTileSerializer :
	public ctb::TerrainSerializer,
	public ctb::MeshSerializer {
public:
	CTBMBTileSerializer(const std::string &outputDir, const std::string &datasetName, bool resume);  

	/// Start a new serialization task
	virtual void startSerialization() {};

	/// Returns if the specified Tile Coordinate should be serialized
	virtual bool mustSerializeCoordinate(const ctb::TileCoordinate *coordinate);

	/// Serialize a TerrainTile to the store
	virtual bool serializeTile(const ctb::TerrainTile *tile);
	/// Serialize a MeshTile to the store
	virtual bool serializeTile(const ctb::MeshTile *tile, bool writeVertexNormals = false);

	/// Serialization finished, releases any resources loaded
	virtual void endSerialization() {};  

  /// Write metadata (layer.json) to mbTiles
  virtual void saveMetadata(const std::stringstream &strm);

  /// Is a tile coordinate in mbTiles?
  virtual bool hasCoordinate(const ctb::TileCoordinate &coordinate);

protected:
  void recordValidPoint(const TileCoordinate & coord);

  bool checkIfAlreadyRendered(const TileCoordinate & coord);

	/// sql db
  std::unique_ptr<MbTilesDb> mbTiles;

  int validTiles;
  int totalTiles;
  int alreadyRendered;
  std::vector<std::vector<TilePoint>> validPoints;

  /// existing tiles
  std::unordered_set<uint64_t> renderedTiles;

	/// The db path
	std::string dbPath;

	/// The target directory where serializing
	std::string moutputDir;
	/// Do not overwrite existing files
	bool mresume;
};

#endif /* CTBMBTILESERIALIZER_HPP */
