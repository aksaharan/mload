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

#ifndef INPUTPROCESSOR_H_
#define INPUTPROCESSOR_H_
#include "inputformat.h"
#include "mongocxxdriver.h"
#include "inputqueue.h"

namespace loader {

    class Loader;
    /*
     * Current assumption is that a single LoadSegmentProcessor handles a single namespace.
     * This could change in the future but to keep lookups down it's probably better to
     * make sure that load segments are handed off by namespace if possible.
     */
    class InputProcessor : public queue::LoadBuilder {
    public:
        InputProcessor(Loader *owner, std::string ns);
        ~InputProcessor() {
        }

        /**
         * Takes a segment and it's logical location (i.e. the mapping to something real)
         * It then puts all the documents into the right queues
         */
        void processSegmentToAggregator(const cpp::LocSegment &segment, cpp::LogicalLoc logicalLoc);

        virtual Bson getFinalDoc();
        virtual Bson getIndex();
        virtual Bson getAdd();
        virtual cpp::DocLoc getLoc();

    private:
        Loader *_owner;
        const std::string _ns;
        queue::LoadQueueHolder _queue;
        cpp::LogicalLoc _docLogicalLoc;
        cpp::DocLoc _docLoc;
        std::string _docJson;
        Bson _doc;
        mongo::BSONObjBuilder *_extra = NULL;
        Bson _docShardKey;bool _added_id {};
        std::unique_ptr<InputFormat> _input;

    };

} /* namespace loader */

#endif /* INPUTPROCESSOR_H_ */
