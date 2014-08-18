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

#ifndef OPQUEUE_H_
#define OPQUEUE_H_

#include <boost/lockfree/queue.hpp>
#include "mongocxxdriver.h"


namespace cpp {
    namespace mtools {
        /*
         * The eventual idea here is to be able to track operations success etc.
         * The functors should allow for an easy retry method etc.
         */
        using WriteConcern = mongo::WriteConcern;
        constexpr auto *DEFAULT_WRITE_CONCERN = &WriteConcern::majority;
        using OpReturnCode = bool;
        using Data = bson::bo;
        using DataQueue = std::vector<Data>;
        using Connection = mongo::ScopedDbConnection;
        struct DbOp {
        public:
            DbOp() {}
            virtual ~DbOp() {}
            virtual OpReturnCode run(Connection &conn) = 0;
        };
        using DbOpPointer = std::unique_ptr<DbOp>;

        class OpQueue {
        public:
            OpQueue() { }
            virtual ~OpQueue() { }

            virtual OpReturnCode push(DbOpPointer &dbOp) = 0;
            virtual OpReturnCode pop(DbOpPointer &dbOp) = 0;
        };

        class OpQueueNoLock : public OpQueue {
        public:

            OpQueueNoLock(size_t queueSize): _queue(queueSize) {}
            ~OpQueueNoLock();


            inline OpReturnCode push(DbOpPointer &dbOp) {
                return _queue.push(std::move(dbOp).release());
            }

            inline OpReturnCode pop(DbOpPointer &dbOp) {
                DbOp *rawptr;
                bool result = _queue.pop(rawptr);
                if(result)
                    dbOp.reset(rawptr);
                return result;
            }

        private:
            //boost::lockfree::queue<DbOp*, boost::lockfree::capacity<50>> _queue;
            boost::lockfree::queue<DbOp*> _queue;
        };

        struct OpQueueBulkInsert : public DbOp {
            OpQueueBulkInsert(std::string ns, DataQueue *data, int flags = 0, const WriteConcern *wc = DEFAULT_WRITE_CONCERN);
            OpReturnCode run(Connection &conn);
            std::string _ns;
            DataQueue _data;
            int _flags;
            const WriteConcern* _wc;

            static DbOpPointer make(std::string ns, DataQueue *data, int flags = 0, const WriteConcern *wc = DEFAULT_WRITE_CONCERN) {
                return DbOpPointer(new OpQueueBulkInsert(ns, data, flags, wc));
            }
        };
    } /*namespace mtools*/
} /* namespace cpp */

#endif /* OPQUEUE_H_ */
