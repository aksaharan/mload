/*    Copyright Charlie Page 2014
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#ifndef MONGOCLUSTER_H_
#define MONGOCLUSTER_H_

#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <string>
#include <mongo/client/dbclient.h>
#include "bsontools.h"
#include "index.h"
#include "tools.h"

namespace cpp {

namespace mtools {

//TODO: replace asserts with exceptions
/*
 * Represents a mongo cluster
 * IT IS CRITICAL THAT CHUNKS ARE SORTED IN ASCENDING ORDER
 */
class MongoCluster {
public:

	static const bson::bo CHUNK_SORT;
    /*Holds shard name vs. connection string*/
	using NameSpace = std::string;
	using ShardName = std::string;
	using ShardConn = std::string;
    using ShardMap = std::unordered_map<ShardName, ShardConn>;
    using ChunkIndexKey = bson::bo;
    /*Holds all nsChunks*/
    using ChunkShardMap = cpp::Index<ChunkIndexKey, ShardMap::iterator, cpp::BSONObjCmp>;
    using NsChunkMap = std::unordered_map<NameSpace, ChunkShardMap>;
    using Mongos = std::vector<std::string>;
    MongoCluster() = delete;
    MongoCluster(std::string conn);
    virtual ~MongoCluster();

    ShardMap& shards() { return _shards; }
    NsChunkMap& nsChunks() { return _nsChunks; }
    Mongos& mongos() { return _mongos; }
    ChunkShardMap& nsChunks(const std::string &ns) { return _nsChunks.at(ns); }

    size_t chunksCount(const std::string &ns) const {
        auto i = _nsChunks.find(ns);
        if(i == _nsChunks.end())
            return 0;
        return i->second.size();
    }

    const std::string& getConn(const std::string &shard) {
        return _shards.at(shard);
    }

    ShardName getShardForChunk(const std::string &ns, const ChunkIndexKey &key) {
        return _nsChunks.at(ns).at(key)->first;
    }

    template <typename T>
    void getShardList(T *queue) const {
        for(auto &i: _shards)
            queue->push_back(i.first);
    }

    friend std::ostream& operator<<(std::ostream &o, const MongoCluster &c);

private:
    std::string _conn;
    Mongos _mongos;
    ShardMap _shards;
    NsChunkMap _nsChunks;

    void clear();
    void load();

};

std::ostream& operator<<(std::ostream &o, MongoCluster &c);

} /* namespace mtools */
} /* namespace cpp */

#endif /* MONGOCLUSTER_H_ */
