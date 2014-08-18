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

#include <algorithm>
#include <cstddef>
#include "mongocluster.h"
#include "mongocxxdriver.h"

namespace cpp {
namespace mtools {

const bson::bo MongoCluster::CHUNK_SORT = BSON("max" << 1);

MongoCluster::MongoCluster(std::string conn): _conn(std::move(conn)) {
    load();
}

MongoCluster::~MongoCluster() { }

void MongoCluster::clear() {
    _shards.clear();
    _nsChunks.clear();
    _mongos.clear();
}

std::ostream& operator<<(std::ostream &o, MongoCluster &c) {
    o << "Shards:" << "\n";
    for(auto &i: c.shards())
        o << i.first << " : " << i.second << "\n";

    o << "\nChunks:" << "\n";
    for(auto &i: c.nsChunks()) {
        o << i.first << "\n";
        for(auto &s: i.second.container())
            o << "\tUpper Bound: " << s.first << " Shard: " << s.second->first << "\n";
    }
    o << "\nMongoS:" << "\n";
    for(auto &i: c.mongos())
        o << i << "\n";

    return o;
}


void MongoCluster::load() {
    clear();
    //TODO: Add a sanity check this is actually a mongoS/ config server
    mongo::DBClientConnection c;
    c.connect(_conn);

    //Load shards
    mongo::Cursor cur(c.query("config.shards"));
    while(cur->more()) {
        bson::bo o = cur->next();
        std::string shard = o.getStringField("_id");
        std::string connect = o.getStringField("host");
        if(shard.empty() || connect.empty())
            throw std::logic_error("Couldn't load shards, empty values, is this a sharded cluster?");
        _shards.emplace(std::move(shard), std::move(connect));
    }

    //Load chunks
    cur = c.query("config.chunks", mongo::Query().sort(BSON("ns" << 1 << "max" << -1)));
    std::string curNs;

    ChunkShardMap *idx = NULL;
    while(cur->more()) {
        bson::bo o = cur->next();
        std::string shard = o.getStringField("shard");
        if(shard.empty())
            throw std::logic_error("Couldn't load chunks, empty shard");
        std::string ns = o.getStringField("ns");
        if(ns.empty())
            throw std::logic_error("Couldn't load chunks, empty namespace");
        if(ns != curNs) {
            if(idx)
                idx->finalize();
            curNs = ns;
            idx = &_nsChunks.emplace(ns,ChunkShardMap(cpp::BSONObjCmp(BSON("max" << 1)))).first->second;
        }
        idx->insertUnordered(o.getField("max").Obj().getOwned(), _shards.find(shard));
    }
    if(idx)
        idx->finalize();

    cur = c.query("config.mongos");
    while(cur->more()) {
        bson::bo o = cur->next();
        _mongos.emplace_back(o.getStringField("_id"));
    }
}

} /* namespace mtools */
} /* namespace cpp */
