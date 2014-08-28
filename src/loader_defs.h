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

#include <deque>
#include <vector>
#include "mongo_cxxdriver.h"
#include "mongo_end_point.h"

namespace loader {
    //TODO: remove once loadqueue and opagg factories are built
    constexpr size_t DIRECT_LOAD = 8;

    using Bson = mongo::BSONObj;
    using BsonV = std::vector<mongo::BSONObj>;
    using BsonQ = std::deque<mongo::BSONObj>;
    using BsonPairDeque = std::deque<std::pair<mongo::BSONObj, mongo::BSONObj>>;
}  //namespace loader


