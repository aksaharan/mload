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

#ifndef LOADERDEFS_H_
#define LOADERDEFS_H_

#include <deque>
#include <vector>
#include "mongocxxdriver.h"
#include "mongoendpointsettings.h"

namespace loader {
    //TODO: remove once loadqueue and opagg factories are built
    constexpr size_t DIRECT_LOAD = 8;

    using Bson = mongo::BSONObj;
    using BsonV = std::vector<mongo::BSONObj>;
    using BsonQ = std::deque<mongo::BSONObj>;
    using BsonPairDeque = std::deque<std::pair<mongo::BSONObj, mongo::BSONObj>>;

    /**
     * Individual queue settings.  These are the queues used by the input classes
     * Common def here so dependencies are clean
     */
    namespace queue {
        struct Settings {
            mongo::BSONObj sortIndex;
            size_t queueSize;
            size_t direct;
            size_t RAM;
        };
    }

    /**
     * Individual opagg settings.  These are the loader queues that aggregate operations
     * Common def here so dependencies are clean
     */
    namespace opagg {
        struct Settings {
            int writeConcern;
            bool directLoad;
            mongo::BSONObj sortIndex;
            size_t queueSize;
            std::string workPath;
            size_t direct;
            size_t RAM;
            size_t workThreads;
        };
    }

    /**
     * Loader settings.
     * Common def here so dependencies aren't circular.
     */
    class Settings {
    public:
        using FieldKeys = std::vector<std::string>;
        using Shards = std::vector<std::string>;
        std::string loadDir;
        std::string fileRegex;
        std::string connection;
        std::string database;
        std::string collection;
        std::string workPath;
        int syncDelay;
        int threads;
        size_t mongoLocklessMissWait;
        bool add_id;
        bool indexHas_id;
        size_t indexPos_id;
        bool hashed;
        int chunksPerShard;
        std::string shardKeyJson;
        bson::bo shardKeysBson;
        FieldKeys shardKeyFields;
        Shards shards;

        queue::Settings queueSettings;
        opagg::Settings opAggSettings;
        cpp::mtools::EndPointSettings endPointSettings;

        bool shard() {
            return !shards.empty();
        }

        std::string ns() const {
            return database + "." + collection;
        }

        /**
         * Check invariants and sets dependent settings
         * Needs to be called once all the user input is read in
         */
        void process() {
            endPointSettings.startImmediate = false;
            indexHas_id = false;
            indexPos_id = size_t( -1 );
            size_t count { };
            shardKeysBson = mongo::fromjson( shardKeyJson );
            for ( bson::bo::iterator i( shardKeysBson ); i.more(); ) {
                bson::be key = i.next();
                if ( key.valueStringData() == std::string( "hashed" ) ) hashed = true;
                else if ( key.Int() != 1 && key.Int() != -1 ) {
                    std::cerr << "Unknown value for key: " << key << "\nValues are 1, -1, hashed"
                              << std::endl;
                    exit( 1 );
                }
                shardKeyFields.push_back( key.fieldName() );
                if ( !indexHas_id && key.fieldNameStringData().toString() == "_id" ) {
                    indexHas_id = true;
                    indexPos_id = count;
                }
                ++count;
            }
            if ( hashed && count > 1 ) {
                std::cerr << "MongoDB currently only supports hashing of a single field"
                          << std::endl;
                exit( 1 );
            }
            if ( !indexHas_id ) add_id = false;
            opAggSettings.sortIndex = shardKeysBson;
            queueSettings.sortIndex = shardKeysBson;

            opAggSettings.workPath = workPath;
            opAggSettings.directLoad = endPointSettings.directLoad;

        }
    };
}

#endif /* LOADERDEFS_H_ */
