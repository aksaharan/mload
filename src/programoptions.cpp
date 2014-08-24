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

#include "programoptions.h"
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <mongoc.h>
#include <thread>

namespace loader {

    namespace {

    struct InitTarget {
        bool dropDb;
        bool dropColl;
        bool stopBalancer;
        int chunksMin;
    };

    std::string toString(const bson_t *bson) {
        char *str = bson_as_json(bson, NULL);
        std::string ret(str);
        bson_free(str);
        return ret;
    }

    } /*namespace*/

//TODO: Cover to C++ driver
//TODO: config file
//TODO: logging queue and output file
//TODO: move the program options setup to the C++ driver
//TODO: option to drop all indexes (i.e. even shard index) and then build afterward
//TODO: option to discard_id (i.e. really fast right inserts on _id)
//TODO: Sync delay, set on all replicas, then reset at end of load
    void setProgramOptions(Settings &settings, int argc, char *argv[]) {
        InitTarget initTarget;
        namespace po = boost::program_options;
        po::options_description generic("Generic");
        po::options_description cmdline("Command Line");
        generic.add_options()
            //TODO: make type, strategy, ordering, loadPath required
            ("help,h", "print help messages")
            //TODO:log file
            ("logFile,l", po::value<std::string>(), "logFile - NOT YET IMPLEMENTED") //want to make sure -l doesn't get taken, so it's here
            ("type,T", po::value<std::string>()->default_value("json"), "load type: json")
            ("loadPath,p", po::value<std::string>(&settings.loadDir)->required(), "directory to load files from")
            ("fileRegex,r", po::value<std::string>(&settings.fileRegex), "regular expression to match files on: (.*)(json)")
            ("workPath", po::value<std::string>(&settings.workPath), "directory to save temporary work in")
            ("uri,u", po::value<std::string>(&settings.connection)->default_value("127.0.0.1:27017"), "mongodb connection URI. See http://docs.mongodb.org/manual/reference/connection-string/")
            ("db,d", po::value<std::string>(&settings.database)->required(), "database")
            ("coll,c", po::value<std::string>(&settings.collection)->required(), "collection")
            ("writeConcern,w", po::value<int>(&settings.dispatchSettings.writeConcern)->default_value(0), "write concern, # of nodes")
            //TODO: Sync delay, need to do it to all replicas
            ("directLoad,D", po::value<bool>(&settings.endPointSettings.directLoad)->default_value(false), "Directly load into mongoD, bypass mongoS")
            ("syncDelay,S", po::value<int>(&settings.syncDelay)->default_value(-1), "NOT YET IMPLEMENTED") //reserving S
            ("batchSize,B", po::value<size_t>(&settings.dispatchSettings.queueSize)->default_value(10000), "size of bson array sent to driver")
            ("inputThreads,t", po::value<int>(&settings.threads)->default_value(0), "threads, 0 for auto limit, -x for a limit from the max hardware threads(default: 0)")
            ("endPointThreads,e", po::value<size_t>(&settings.endPointSettings.threadCount)->default_value(3), "threads per end point")
            ("endPointLocklessMissWait", po::value<size_t>(&settings.endPointSettings.sleepTime)->default_value(10), "Wait time for mongo connections with a lockless miss method")
            ("endPointQueueSize", po::value<size_t>(&settings.endPointSettings.queueSize)->default_value(10), "Lockless queue size")
            ("opAggThreads", po::value<size_t>(&settings.dispatchSettings.workThreads)->default_value(10), "Threads available to the Operations Aggregator to do work (i.e. spill to disk)")
            ("readQueueSize", po::value<long unsigned int>(&settings.aggregatorSettings.queueSize)->default_value(10000), "Read queue size")
            ("dropDb", po::value<bool>(&initTarget.dropDb)->default_value(false), "DANGER: Drop the database")
            ("dropColl", po::value<bool>(&initTarget.dropColl)->default_value(false), "DANGER: Drop the collection")
            ("shard,s", po::value<bool>()->default_value(true), "Used a sharded setup")
            ("stopBalancer", po::value<bool>(&initTarget.stopBalancer)->default_value(false), "stop the balancer")
            ("sortKey,k", po::value<std::string>(&settings.shardKeyJson)->required(), "Dotted fields not supported (i.e. subdoc.field) must quote fields (\"_id\":\"hashed\"")
            ("add_id", po::value<bool>(&settings.add_id)->default_value(true), "Add _id if it doesn't exist, operations will error if _id is required")
            ("initialChunksPerShard,i", po::value<int>(&settings.chunksPerShard)->default_value(-1), "Number of initial chunks")
            //TODO: wait for presplit should change to total = inital and distribution is <= 1 between all shards and everything is stable for 5s.
            ("waitForPresplit,W", po::value<int>(&initTarget.chunksMin)->default_value(-1), "Wait for number of chunks per shard, defaults to 2 if shardColl is using a hash, if sortJson isn't hashed **YOU** must start the presplit")
            ;
        cmdline.add_options()
            ("config", po::value<std::string>(), "config file - NOT YET IMPLEMENTED")
            ;
        cmdline.add(generic);
        po::variables_map vm;
        std::string errormsg;
        try {
            po::store(po::command_line_parser(argc, argv).options(cmdline).run(), vm);
            /*if(vm.count("config"))
             po::store(po::parse_config_file(vm["config"].as<std::string>().c_str()), vm));*/
            po::notify(vm);
        }
        catch (std::exception &e) {
            errormsg = e.what();
        }
        if (vm.count("help") || !errormsg.empty()) {
            cmdline.print(std::cout);
            if (!errormsg.empty()) std::cerr << "Unable to parse options: " + errormsg << std::endl;
            exit(0);
        }

        settings.process();
        if (settings.endPointSettings.directLoad) initTarget.stopBalancer = true;

        if (!boost::starts_with(settings.connection, "mongodb://")) settings.connection.insert(0,
                                                                                               "mongodb://");
        //TODO: verify JSON
        if (!settings.shardKeyJson.empty() && !boost::starts_with(settings.shardKeyJson, "{")) {
            settings.shardKeyJson.insert(0, "{").append("}");
        }
        //Set number of threads to use
        if (settings.threads == 0) settings.threads = std::thread::hardware_concurrency();
        else if (settings.threads < 0) settings.threads = std::thread::hardware_concurrency()
            + settings.threads;

        if (settings.chunksPerShard <= 0 && settings.hashed) settings.chunksPerShard = 2;

        if (initTarget.chunksMin < 0) initTarget.chunksMin = settings.chunksPerShard;

        if (!is_directory(boost::filesystem::path(settings.loadDir))) {
            std::cerr << "loadPath is required to be a directory. loadPath: " << settings.loadDir
                      << std::endl;
            exit(1);
        }

        bson_error_t error;
        mongoc_client_t *client;
        client = mongoc_client_new(settings.connection.c_str());
        if (!client) {
            std::cerr << "Unable to connect to " << settings.connection << std::endl;
            exit(1);
        }

        //If this is a sharded setup, then verify that the config information exists and get shards
        //TODO: Should start the fakeout here for a replica set
        if (vm["shard"].as<bool>()) {
            bson_t *query;
            query = BCON_NEW("$query", "{", "}"); //bson_new();
            mongoc_collection_t *coll;
            coll = mongoc_client_get_collection(client, "config", "shards");
            mongoc_cursor_t *cursor;
            cursor = mongoc_collection_find(coll, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);
            if (!cursor) {
                std::cerr << "Error query for shards failed" << std::endl;
                exit(1);
            }
            const bson_t *shard;
            while (mongoc_cursor_more(cursor) && mongoc_cursor_next(cursor, &shard)) {
                bson_iter_t iter;
                assert(bson_iter_init(&iter, shard));
                assert(bson_iter_find(&iter, "host"));
                uint32_t size;
                std::string shardConn(bson_iter_utf8(&iter, &size));
                settings.shards.push_back(shardConn);
            }
            if (settings.shards.empty()) {
                std::cerr << "Unable to locate shards for a sharded setup" << std::endl;
                exit(1);
            }
            mongoc_cursor_destroy(cursor);
            mongoc_collection_destroy(coll);
            bson_destroy(query);
        }
        else {
            throw std::logic_error("Only supports sharded setups");
        }

        //Stop the balancer if required
        if (initTarget.stopBalancer) {
            bson_t *query, *updater;
            query = BCON_NEW("_id", "balancer");
            updater = BCON_NEW("$set", "{", "stopped", "true", "}");
            mongoc_collection_t *coll;
            coll = mongoc_client_get_collection(client, "config", "settings");
            assert(coll);
            if (!mongoc_collection_update(coll, MONGOC_UPDATE_UPSERT, query, updater,
            NULL,
                                          &error))
            {
                std::cerr << "Error unable to stop balancer: " << error.message << std::endl;
                exit(1);
            }
            bson_destroy(query);
            bson_destroy(updater);
            mongoc_collection_destroy(coll);
        }

        //If that database needs to be dropped/sharded? do it
        if (initTarget.dropColl || initTarget.dropDb || settings.shard()) {
            bson_t *bsonCmd;

            if (initTarget.dropDb) {
                std::cout << "Dropping database" << std::endl;
                bsonCmd = BCON_NEW("dropDatabase", BCON_INT32(1));
                if (!mongoc_client_command_simple(client, settings.database.c_str(), bsonCmd,
                NULL,
                                                  NULL, &error)) std::cerr
                        << "Error dropping database (does it exist?): " << error.message
                        << std::endl;
                bson_destroy(bsonCmd);
            }
            else if (initTarget.dropColl) {
                std::cout << "Dropping collection" << std::endl;
                bsonCmd = BCON_NEW("drop", settings.collection.c_str());
                if (!mongoc_client_command_simple(client, settings.database.c_str(), bsonCmd,
                NULL,
                                                  NULL, &error)) std::cerr
                        << "Error dropping collection (does it exist?): " << error.message
                        << std::endl;
                bson_destroy(bsonCmd);
            }

            //If sharding, ensure that the collection is sharded and all chunks are properly setup
            if (settings.shard()) {
                if (!settings.shardKeyJson.empty()) {
                    std::cout << "Sharding collection" << std::endl;
                    //TODO: Check to see if sharding is already enabled and skip the command
                    //TODO: Check to see if collection exists and if so keys match
                    bsonCmd = BCON_NEW("enableSharding", settings.database.c_str());
                    if (!mongoc_client_command_simple(client, "admin", bsonCmd,
                    NULL,
                                                      NULL, &error))
                    {
                        std::cerr << "Error sharding database: " << error.message << std::endl;
                    }
                    bson_destroy(bsonCmd);
                    //Create sharded collection
                    //TODO: Allow for using already existing collection: 1)Check to see if it exists, if that is true, make sure the shard key matches the user supplied key or use the shard key
                    bson_t *key = bson_new_from_json(reinterpret_cast<const uint8_t*>(settings
                                                             .shardKeyJson.c_str()),
                                                     settings.shardKeyJson.size(),
                                                     &error);
                    if (!key) {
                        std::cerr << "Error converting key(" << settings.shardKeyJson
                                  << ") to bson: " << error.message << std::endl;
                        exit(1);
                    }
                    if (settings.chunksPerShard > 0 && settings.hashed) bsonCmd =
                            BCON_NEW("shardCollection",
                                     (settings.database + "." + settings.collection).c_str(),
                                     "key",
                                     BCON_DOCUMENT(key),
                                     "numInitialChunks",
                                     BCON_INT32(settings.chunksPerShard * settings.shards.size()));
                    else {
                        bsonCmd = BCON_NEW("shardCollection",
                                           (settings.database + "." + settings.collection).c_str(),
                                           "key",
                                           BCON_DOCUMENT(key));
                        //TODO: the presplits
                    }
                    if (!mongoc_client_command_simple(client, "admin", bsonCmd,
                    NULL,
                                                      NULL, &error))
                    {
                        std::cerr << "Error sharding collection: " << error.message << std::endl;
                        //If we were supposed to have dropped the collection, bail.  We should be able to create and shard it.
                        if (initTarget.dropColl) {
                            std::cerr
                                    << "Error sharding a collection that should have been dropped.  Exiting."
                                    << std::endl;
                            exit(1);
                        }
                    }
                    bson_destroy(bsonCmd);
                    bson_destroy(key);
                }
                else {
                    throw std::logic_error("We currently don't support getting the shard key");
                    //TODO: ensure the collection exists and pull the shard key
                }
            }
        }

        //Ensure the balancer is stopped
        if (initTarget.stopBalancer) {
            std::cout << "Stopping balancer" << std::endl;
            bson_t *selector = BCON_NEW("_id", "balancer");
            bson_t *fields = BCON_NEW("state", BCON_INT32(1));
            mongoc_collection_t *coll;
            coll = mongoc_client_get_collection(client, "config", "locks");
            assert(coll);
            bson_t *matchExpr = BCON_NEW("state", "{", "$gt", BCON_INT32(0), "}");
            mongoc_matcher_t *balancerRunning = mongoc_matcher_new(matchExpr, &error);
            if (!balancerRunning) {
                std::cerr << "Error creating match expression: " << error.message << std::endl;
                exit(1);
            }
            bool first = true;
            mongoc_cursor_t *cursor;
            for (;;) {
                cursor = mongoc_collection_find(coll, MONGOC_QUERY_NONE, 0, 0, 1, selector, fields,
                NULL);
                if (!cursor) {
                    std::cerr << "Error query for balancer failed" << std::endl;
                    exit(1);
                }
                const bson_t *balancerState;
                if (mongoc_cursor_next(cursor, &balancerState)) {
                    if (!mongoc_matcher_match(balancerRunning, balancerState)) break;
                }
                else //assuming there aren't any balancer documents it's not running
                break;
                if (first) {
                    first = false;
                    std::cout << "Waiting on balancer to stop";
                }
                std::cout << "." << std::flush;
                mongoc_cursor_destroy(cursor);
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
            if (!first) std::cout << std::endl;
            bson_destroy(matchExpr);
            mongoc_matcher_destroy(balancerRunning);
            mongoc_cursor_destroy(cursor);
            bson_destroy(selector);
            bson_destroy(fields);
            mongoc_collection_destroy(coll);
        }

        //if the setup is hashed, wait for chunks to distribute
        if (initTarget.chunksMin > 0 && settings.hashed) {
            std::cout << "Settings up hashed presplits" << std::endl;
            mongoc_collection_t *coll = mongoc_client_get_collection(client, "config", "chunks");
            bson_t *pipeline = BCON_NEW("pipeline",
                                        "[",
                                        "{",
                                        "$match",
                                        "{",
                                        "ns",
                                        settings.ns().c_str(),
                                        "}",
                                        "}",
                                        "{",
                                        "$group",
                                        "{",
                                        "_id",
                                        "$shard",
                                        "chunkCount",
                                        "{",
                                        "$sum",
                                        BCON_INT32(1),
                                        "}",
                                        "}",
                                        "}",
                                        "]");
            mongoc_cursor_t *cursor;
            //TODO: fix this when matcher is working
            bson_t *matchExpr = BCON_NEW("chunkCount",
                                         "{",
                                         "$gt",
                                         BCON_INT32(initTarget.chunksMin - 1),
                                         "}");
            mongoc_matcher_t *minChunk = mongoc_matcher_new(matchExpr, &error);
            int loopCount = 0;
            for (;;) {
                cursor = mongoc_collection_aggregate(coll, MONGOC_QUERY_NONE, pipeline,
                NULL,
                                                     NULL);
                assert(cursor);
                const bson_t *doc;
                int fail = 0;
                while (mongoc_cursor_next(cursor, &doc)) {
                    if (!mongoc_matcher_match(minChunk, doc)) ++fail;
                }
                if (mongoc_cursor_error(cursor, &error)) {
                    std::cerr << "Error checking chunks aggregation cursor failed: "
                              << error.message << std::endl;
                    exit(1);
                }
                if (fail == 0) break;
                if (!loopCount) {
                    std::cout << "Waiting for chunks to balance (min " << initTarget.chunksMin
                              << " chunks per shard)";
                }
                mongoc_cursor_destroy(cursor);
                if (!(++loopCount % 60)) std::cout
                        << fail
                        << " shard(s) behind (manual intervention required for balancing to complete?)"
                        << std::flush;
                else std::cout << "." << std::flush;
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
            mongoc_cursor_destroy(cursor);
            mongoc_matcher_destroy(minChunk);
            mongoc_collection_destroy(coll);
            bson_destroy(matchExpr);
            bson_destroy(pipeline);
        }
        mongoc_client_destroy(client);
    }

} /* namespace loader */
