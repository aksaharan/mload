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

#pragma once

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
         * All required cluster information can be taken from here.  (i.e. this is the config database)
         * IT IS CRITICAL THAT CHUNKS ARE SORTED IN ASCENDING ORDER
         * All other operations rely on upper_bound/sort being correct and they load the chunks
         * from here.
         */
        class MongoCluster {
        public:
            //The sort for chunks.  "max": 1
            static const bson::bo CHUNK_SORT;
            using NameSpace = std::string;
            using ShardName = std::string;
            using ShardConn = std::string;
            using ShardMap = std::unordered_map<ShardName, ShardConn>;
            using ChunkIndexKey = bson::bo;
            /*Holds the shard each chunk is on*/
            using ChunkShardMap = cpp::Index<ChunkIndexKey, ShardMap::iterator, cpp::BSONObjCmp>;
            /*Holds chunks for each name space*/
            using NsChunkMap = std::unordered_map<NameSpace, ChunkShardMap>;
            using Mongos = std::vector<std::string>;
            MongoCluster() = delete;
            explicit MongoCluster(std::string conn);
            virtual ~MongoCluster();

            /**
             * @return access to shards and their connection strings
             */
            ShardMap& shards() {
                return _shards;
            }

            /**
             * @return access to namespace chunks
             */
            NsChunkMap& nsChunks() {
                return _nsChunks;
            }

            /**
             * All chunks for a single namespace
             */
            ChunkShardMap& nsChunks(const std::string &ns) {
                return _nsChunks.at(ns);
            }

            /**
             * access to mongos.
             */
            Mongos& mongos() {
                return _mongos;
            }

            /**
             * @return count of chunks for a single namespace
             */
            size_t chunksCount(const std::string &ns) const {
                auto i = _nsChunks.find(ns);
                if (i == _nsChunks.end()) return 0;
                return i->second.size();
            }

            /**
             * @return connection string for a shard
             */
            const std::string& getConn(const std::string &shard) {
                return _shards.at(shard);
            }

            /**
             * @return given a namespace and chunk give back the shard it resides on
             */
            ShardName getShardForChunk(const std::string &ns, const ChunkIndexKey &key) {
                return _nsChunks.at(ns).at(key)->first;
            }

            /**
             * Appends to a container a list of the shards.
             * The container is NOT cleared.
             */
            template<typename T>
            void getShardList(T *queue) const {
                for (auto &i : _shards)
                    queue->push_back(i.first);
            }

            /**
             * Writes the chunk config to the ostream
             */
            friend std::ostream& operator<<(std::ostream &o, const MongoCluster &c);

        private:
            std::string _conn;
            Mongos _mongos;
            ShardMap _shards;
            NsChunkMap _nsChunks;

            /**
             * clears all values for the loaded cluster
             */
            void clear();

            /**
             * loads values from the cluster from the _conn string
             */
            void load();

        };

        std::ostream& operator<<(std::ostream &o, MongoCluster &c);

    }  //namespace mtools
}  //namespace cpp
