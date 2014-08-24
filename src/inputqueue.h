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

#include <memory>
#include <unordered_map>
#include "index.h"
#include "loaderdefs.h"
#include "mongocxxdriver.h"
#include "mongocluster.h"
#include "chunkdispatch.h"

namespace loader {
    namespace queue {

        class LoadQueueHolder;
        class LoadQueue;
        class DirectLoadQueue;

        enum LoadQueueTypes {
            DIRECT = 0, RAM, DISK, INDEXONLY
        };

        using LoadQueuePointer = std::unique_ptr<LoadQueue>;

        //Insert data needs to hold the index, location, and any generated information for the object, i.e. _id
        using Key = Bson;

        class LoadBuilder {
        public:
            LoadBuilder() {
            }
            ;
            virtual ~LoadBuilder() {
            }
            ;
            virtual Bson getFinalDoc() = 0;
            virtual Bson getIndex() = 0;
            virtual Bson getAdd() = 0;
            virtual cpp::DocLoc getLoc() = 0;
        };

        /*
         * Public interface for getting bson documents into large batches by chunk
         * Documents should be pushed into.
         */
        class LoadQueue {
        public:
            /**
             * Push is called when the LoadBuilder is ready to have any values required read
             */
            virtual void push(LoadBuilder *stage) = 0;

            /**
             * Is the queue empty?
             */
            virtual bool empty() = 0;

            /**
             *  Makes a final push to clear the queue
             */
            virtual void clean() = 0;

            virtual ~LoadQueue() {
            }

            /**
             * @return the opAggregator that the queue should post to
             */
            dispatch::AbstractChunkDispatch* postTo() {
                return _opAgg;
            }

            /**
             * @return the holder
             */
            LoadQueueHolder* owner() {
                return _owner;
            }

            size_t queueSize() {
                return _queueSize;
            }

            /**
             * @return the index upper bound being used
             */
            const Bson& UBIndex() {
                return _UBIndex;
            }

        protected:
            LoadQueue(LoadQueueHolder *owner, const Bson &UBIndex);

        private:
            LoadQueueHolder *_owner;
            size_t _queueSize;
            dispatch::AbstractChunkDispatch *_opAgg;
            const Bson _UBIndex;
        };

        /**
         * LoadQueueHodler is lock free.  It aggregates documents into batches for passing onto
         * an operation aggregator.  This is only valid for a single namespace.
         */
        class LoadQueueHolder {
        public:
            LoadQueueHolder(Settings settings,
                            cpp::mtools::MongoCluster &mCluster,
                            dispatch::ChunkDispatcher *out,
                            cpp::mtools::MongoCluster::NameSpace ns) :
                    _settings(std::move(settings)),
                    _mCluster(mCluster),
                    _out(out),
                    _inputPlan(cpp::mtools::MongoCluster::CHUNK_SORT),
                    _ns(ns)
            {
                init(_ns);
            }

            ~LoadQueueHolder() {
                clean();
            }

            /**
             * @return the stage for a single bson value.
             */
            //TODO: Look at forcing more localization on the search
            LoadQueue* getStage(const Bson &indexValue) {
                return _inputPlan.upperBound(indexValue).get();
            }

            /**
             * returns the opAggregator for that upper bound chunk key
             */
            dispatch::AbstractChunkDispatch* getOpAggForChunk(Key key) {
                return out()->at(key).get();
            }

            /**
             * @return the settings this LoadQueueHolder is using
             */
            const Settings& settings() const {
                return _settings;
            }

        private:
            using InputPlan = cpp::Index<cpp::mtools::MongoCluster::ChunkIndexKey, LoadQueuePointer, cpp::BSONObjCmp>;

            /**
             * Sets up a single name space
             */
            void init(const cpp::mtools::MongoCluster::NameSpace &ns);

            /**
             * Clear the queues
             */
            void clean();

            cpp::mtools::MongoCluster& cluster() {
                return _mCluster;
            }
            dispatch::ChunkDispatcher *out() {
                return _out;
            }

            Settings _settings;
            cpp::mtools::MongoCluster &_mCluster;
            dispatch::ChunkDispatcher *_out;
            InputPlan _inputPlan;
            cpp::mtools::MongoCluster::NameSpace _ns;

        };

        class DirectLoadQueue : public LoadQueue {
        public:
            DirectLoadQueue(LoadQueueHolder *owner, Bson UBIndex) :
                    LoadQueue(owner, UBIndex)
            {
                _bsonHolder.reserve(queueSize());
            }

            void push(LoadBuilder *stage) {
                _bsonHolder.push_back(stage->getFinalDoc());
                if (_bsonHolder.size() > queueSize()) {
                    postTo()->push(&_bsonHolder);
                    _bsonHolder.reserve(queueSize());
                }
            }

            void clean() {
                if (!_bsonHolder.empty()) postTo()->push(&_bsonHolder);
            }

            static LoadQueuePointer create(LoadQueueHolder *owner, Bson UBIndex) {
                return LoadQueuePointer(new DirectLoadQueue(owner, UBIndex));
            }

        private:
            BsonV _bsonHolder;

            bool empty() {
                return _bsonHolder.empty();
            }
        };

        class RAMLoadQueue : public LoadQueue {
        public:
            RAMLoadQueue(LoadQueueHolder *owner, const Bson &UBIndex, const Bson &index) :
                    LoadQueue(owner, UBIndex)
            {
            }

            void push(LoadBuilder *stage) {
                _bsonHolder.push_back(std::make_pair(stage->getIndex(), stage->getFinalDoc()));
                if (_bsonHolder.size() > queueSize()) {
                    postTo()->pushSort(&_bsonHolder);
                }
            }

            void clean() {
                if (!_bsonHolder.empty()) postTo()->pushSort(&_bsonHolder);
            }

            bool empty() {
                return _bsonHolder.empty();
            }

            static LoadQueuePointer create(LoadQueueHolder *owner,
                                           const Bson &UBIndex,
                                           const Bson &index)
            {
                return LoadQueuePointer(new RAMLoadQueue(owner, UBIndex, index));
            }

        private:
            BsonPairDeque _bsonHolder;

        };

        /*
         * work in progress, ignore
         * being use to examine different disk queues, currently all of them are too disk intensive
         */
        class IndexedBucketQueue : public DirectLoadQueue {
        public:
            IndexedBucketQueue(LoadQueueHolder *owner, const Bson &UBIndex) :
                    DirectLoadQueue(owner, UBIndex)
            {
            }

            static LoadQueuePointer create(LoadQueueHolder *owner,
                                           const Bson &UBIndex,
                                           const Bson &index)
            {
                return LoadQueuePointer(new IndexedBucketQueue(owner, UBIndex));
            }
        };

    } /* namespace queue */
} /* namespace loader */

