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

#include <fstream>
#include <memory>
#include "concurrentcontainer.h"
#include "index.h"
#include "loaderdefs.h"
#include "mongocluster.h"
#include "loaderendpoint.h"

namespace loader {
    namespace dispatch {
        class ChunkDispatcher;

        /**
         * Public interface for the AbstractChunkDispatchs.
         * Locking can be used.  Containers should push operations in batches.
         */
        class AbstractChunkDispatch {
        public:
            struct Settings {
                ChunkDispatcher *owner;
                EndPointHolder *eph;
                Bson chunkUB;
            };

            AbstractChunkDispatch(Settings settings);
            virtual ~AbstractChunkDispatch() {
            }

            /**
             * Sends data to the aggregator from the simple queue
             */
            virtual void push(BsonV *q) = 0;

            /**
             * Sends data to the aggregator from the simple queue
             */
            virtual void pushSort(BsonPairDeque *q) = 0;

            /**
             * Called after any new input is over
             */
            virtual void prep() = 0;

            /**
             * Completely queues the queue into the end points
             */
            virtual void doLoad() = 0;

            /**
             * @return the end point the derived class should send to
             */
            EndPoint* endPoint() {
                return _ep;
            }

            /**
             * @return the holder for the derived class to use
             */
            ChunkDispatcher* owner() {
                return _settings.owner;
            }

            /**
             * @return settings for the derived class to use
             */
            const Settings& settings() const {
                return _settings;
            }

        protected:
            /**
             * Derived classes call this to unload their queues in batches
             */
            void send(cpp::mtools::DataQueue *q);

        private:
            Settings _settings;
            EndPoint *_ep;
        };

        using ChunkDispatchPointer = std::unique_ptr<AbstractChunkDispatch>;

        /**
         * Holds AbstractChunkDispatchs for a namespace
         */
        class ChunkDispatcher {
        public:
            using OrderedWaterFall = std::deque<AbstractChunkDispatch*>;
            using Key = cpp::mtools::MongoCluster::ChunkIndexKey;
            using Value = ChunkDispatchPointer;
            using LoadPlan = cpp::Index<Key, Value, cpp::BSONObjCmp>;
            ChunkDispatcher(dispatch::Settings settings,
                               cpp::mtools::MongoCluster &mCluster,
                               EndPointHolder *eph,
                               cpp::mtools::MongoCluster::NameSpace ns);

            ~ChunkDispatcher() {
                _tp.terminateInitiate();
                _tp.joinAll();
            }

            Value& at(const Key &key) {
                return _loadPlan.at(key);
            }

            const cpp::mtools::MongoCluster::NameSpace& ns() const {
                return _ns;
            }

            cpp::mtools::MongoCluster::ShardName getShardForChunk(Key &key) {
                return _mCluster.getShardForChunk(ns(), key);
            }

            /**
             * @return the AbstractChunkDispatch for a chunk in this namespace
             */
            AbstractChunkDispatch* getOpAggForChunk(Key &key) {
                return _loadPlan.at(key).get();
            }

            /**
             * @return EndPoint to a mongoS in a round robin fashion
             */
            //TODO: test cycling mongoS at startup vs cycling the insert packets, put a mongoS iterator into each opaggreagor and cycle
            EndPoint* getMongoSCycle() {
                return _eph->getMongoSCycle();
            }

            /**
             * @return EndPoint for a specific chunk's max key
             */
            EndPoint* getEndPointForChunk(Key &key) {
                return _eph->at(getShardForChunk(key));
            }

            /*
             * This function assumes ascending order of the chunks by shard chunk in _mCluster for
             * this ns.
             * This ensures that there is the least amount of wait time as possible
             */
            OrderedWaterFall getWaterFall();

            const size_t queueSize() const {
                return _settings.queueSize;
            }

            const Bson& sortIndex() const {
                return _settings.sortIndex;
            }

            /**
             * @return temporary work path for e.g. external sorts
             */
            const std::string& workPath() const {
                return _settings.workPath;
            }

            /**
             * Are direct load queues in use?
             */
            const bool directLoad() const {
                return _settings.directLoad;
            }

            /**
             * @return pointer to the write concern for this OpAggegator queue?
             */
            const mongo::WriteConcern* writeConcern() const {
                return &_wc;
            }

            /**
             * Queues a task in the thread pool associated with this queue
             * Will be used for disk queues
             */
            void queueTask(cpp::ThreadFunction func) {
                _tp.queue(func);
            }

        private:
            void init();

            dispatch::Settings _settings;
            //The thread pool is only used with disk sorting
            cpp::ThreadPool _tp;
            cpp::mtools::MongoCluster &_mCluster;
            EndPointHolder *_eph;
            const cpp::mtools::MongoCluster::NameSpace _ns;
            LoadPlan _loadPlan;
            mongo::WriteConcern _wc;

        };

        inline void AbstractChunkDispatch::send(cpp::mtools::DataQueue *q) {
            endPoint()->push(cpp::mtools::OpQueueBulkInsertUnordered::make(owner()->ns(),
                                                                           q,
                                                                           0,
                                                                           owner()->writeConcern()));
        }

        /**
         * This AbstractChunkDispatch by passes queueing at this stage and send the load directly to the
         * end point.
         */
        class ImmediateDispatch : public AbstractChunkDispatch {
        public:
            ImmediateDispatch(Settings settings) :
                    AbstractChunkDispatch(std::move(settings))
            {
            }

            void push(BsonV *q) {
                send(q);
                //TODO: remove this check
                assert(q->empty());
            }

            void pushSort(BsonPairDeque *q) {
                assert(false);
            }

            /*
             * This OpAgg does nothing else
             */
            void prep() {
            }

            void doLoad() {
            }

            static ChunkDispatchPointer create(ChunkDispatcher *owner, EndPointHolder *eph, Bson chunkUB)
            {
                return ChunkDispatchPointer(new ImmediateDispatch(Settings {owner, eph, chunkUB}));
            }
        };

        /**
         * Stores the data in RAM until it is time to push.  At which point is sorts it and sends it.
         */
        class RAMQueueDispatch : public AbstractChunkDispatch {
        public:
            RAMQueueDispatch(Settings settings) :
                    AbstractChunkDispatch(std::move(settings))
            {
            }

            void push(BsonV *q) {
                assert(false);
            }

            void pushSort(BsonPairDeque *q) {
                //TODO: see if pre sorting is faster
                _queue.moveIn(q);
                q->clear();
            }

            void prep() {
                _queue.sort(Compare(cpp::BSONObjCmp(owner()->sortIndex())));
            }

            void doLoad();

            static ChunkDispatchPointer create(ChunkDispatcher *owner, EndPointHolder *eph, Bson chunkUB)
            {
                return ChunkDispatchPointer(new RAMQueueDispatch(Settings {owner, eph, chunkUB}));
            }

        private:
            using Compare = cpp::IndexPairCompare<cpp::BSONObjCmp, Bson>;

            cpp::ConcurrentQueue<BsonPairDeque::value_type> _queue;

        };

        //TODO: DiskQueue OpAgg, cycle sort?
        class DiskQueueDispatch : public AbstractChunkDispatch {
        public:
            DiskQueueDispatch(Settings settings) :
                    AbstractChunkDispatch(std::move(settings))
            {
                assert(false);
                diskQueue.open(owner()->workPath() + "chunk" + this->settings().chunkUB.toString()
                    + ".bson");
            }

            void push(BsonV *q) {
                _holder.push(std::move(*q));
                owner()->queueTask([this] {this->spill();});
            }

            void pushSort(BsonPairDeque *q) {
                assert(false);
            }

            void prep() {
                //needs to work
                assert(false);
            }

            void doLoad() {
                //needs to work
                assert(false);
            }

            static ChunkDispatchPointer create(ChunkDispatcher *owner, EndPointHolder *eph, Bson chunkUB)
            {
                return ChunkDispatchPointer(new DiskQueueDispatch(Settings {owner, eph, chunkUB}));
            }

        protected:
            void spill() {
                BsonV save;
                while (_holder.pop(save)) {

                }

            }

        private:
            cpp::ConcurrentQueue<BsonV> _holder;
            std::fstream diskQueue;
        };

    }
} /* namespace loader */

