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

#include "opaggregator.h"

namespace loader {
    namespace opagg {

        OpAggregator::OpAggregator( OpSettings settings ) :
                _settings( std::move( settings ) )
        {
            if ( _settings.owner->directLoad() ) _ep =
                    _settings.owner->getEndPointForChunk( _settings.chunkUB );
            else _ep = _settings.owner->getMongoSCycle();
        }

        OpAggregatorHolder::OpAggregatorHolder( opagg::Settings settings,
                                                cpp::mtools::MongoCluster &mCluster,
                                                EndPointHolder *eph,
                                                cpp::mtools::MongoCluster::NameSpace ns ) :
                _settings( std::move( settings ) ),
                _tp( _settings.workThreads ),
                _mCluster( mCluster ),
                _eph( eph ),
                _ns( std::move( ns ) ),
                _loadPlan( _settings.sortIndex )
        {
            _wc.nodes( _settings.writeConcern );
            init();
        }

        void OpAggregatorHolder::init() {
            std::unordered_map<cpp::mtools::MongoCluster::ShardName, size_t> shardChunkCounters;
            //Assuming that nsChunks is in sorted order, or the stage setup won't work right
            for ( auto &iCm : _mCluster.nsChunks( ns() ) ) {
                _loadPlan.insertUnordered( std::get<0>( iCm ), OpAggPointer { } );
                size_t depth = ++( shardChunkCounters[std::get<1>( iCm )->first] );
                if ( depth <= DIRECT_LOAD )
                //TODO: change this to a factory that creates queues based on chunk depth in the shard
                _loadPlan.back() = BypassOpAgg::create( this, _eph, std::get<0>( iCm ) );
                else _loadPlan.back() = RAMQueueOpAgg::create( this, _eph, std::get<0>( iCm ) );
            }
        }

        OpAggregatorHolder::OrderedWaterFall OpAggregatorHolder::getWaterFall() {
            std::unordered_map<cpp::mtools::MongoCluster::ShardName, std::deque<OpAggregator*>> chunksort;
            for ( auto &i : _mCluster.nsChunks( _ns ) )
                chunksort[std::get<1>( i )->first].emplace_back( getOpAggForChunk( std::get<0>( i ) ) );
            OrderedWaterFall wf;
            for ( ;; ) {
                bool added = false;
                for ( auto &i : chunksort ) {
                    auto &q = i.second;
                    if ( q.size() ) {
                        added = true;
                        wf.push_back( q.back() );
                        q.pop_back();
                    }
                }
                if ( !added ) break;
            }
            return wf;
        }

        void RAMQueueOpAgg::doLoad() {
            cpp::mtools::DataQueue sendQueue;
            size_t queueSize = owner()->queueSize();
            for ( auto &i : _queue.unSafeAccess() ) {
                sendQueue.emplace_back( i.second );
                if ( sendQueue.size() >= queueSize ) {
                    send( &sendQueue );
                    sendQueue.clear();
                    sendQueue.reserve( queueSize );
                }
            }
        }
    }
} /* namespace loader */
