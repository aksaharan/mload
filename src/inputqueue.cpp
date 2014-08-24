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

#include "inputqueue.h"

namespace loader {
    namespace aggregator {
        LoadQueue::LoadQueue(LoadQueueHolder *owner, Bson UBIndex) :
                _owner(owner),
                _queueSize(_owner->settings().queueSize),
                _opAgg(_owner->getOpAggForChunk(UBIndex)),
                _UBIndex(std::move(UBIndex))
        {
        }

        void LoadQueueHolder::init(const cpp::mtools::MongoCluster::NameSpace &ns) {
            std::unordered_map<cpp::mtools::MongoCluster::ShardName, size_t> shardChunkCounters;
            //Assumes that nsChunks is in sorted order, or the stage setup won't work right
            for (auto &iCm : _mCluster.nsChunks(ns)) {
                _inputPlan.insertUnordered(std::get<0>(iCm), LoadQueuePointer {});
                size_t depth = ++(shardChunkCounters[std::get<1>(iCm)->first]);
                //TODO: change this to a factory that creates queues based on chunk depth in the shard
                if (depth <= DIRECT_LOAD)
                    _inputPlan.back() = DirectLoadQueue::create(this, std::get<0>(iCm));
                else
                    _inputPlan.back() = RAMLoadQueue::create(this, std::get<0>(iCm));
            }
        }

        void LoadQueueHolder::clean() {
            for (auto &i : _inputPlan)
                i.second->clean();
        }
    } /* namespace queue */
} /* namespace loader */
