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

#include "inputprocessor.h"
#include "loader.h"
#include "util/hasher.h"

namespace loader {

    InputProcessor::InputProcessor(Loader *owner, std::string ns) :
            _owner(owner),
            _ns(ns),
            _queue(_owner->queueSettings(), owner->cluster(), &owner->opAgg(), ns),
            _docLogicalLoc {}
    {
    }

    Bson InputProcessor::getFinalDoc() {
        if (_extra->iterator().more()) {
            if (_added_id) {
                _extra->appendElements(_doc);
                _doc = _extra->obj();
            }
            else
            //TODO: Handle the case were we aren't adding _id and it is in _doc, put _id first
            assert(_added_id);
        }
        return std::move(_doc);
    }

    Bson InputProcessor::getIndex() {
        return std::move(_docShardKey);
    }

    Bson InputProcessor::getAdd() {
        return std::move(_extra->obj());
    }

    cpp::DocLoc InputProcessor::getLoc() {
        _docLoc.length = _input->pos() - _docLoc.start;
        _docLoc.length--;
        assert(_docLoc.length > 0);
        return std::move(_docLoc);
    }

    void InputProcessor::processSegmentToAggregator(const cpp::LocSegment &segment,
                                               cpp::LogicalLoc logicalLoc)
    {
        _docLogicalLoc = logicalLoc;
        //May need to switch to void getFields(unsigned n, const char **fieldNames, BSONElement *fields) const;
        const bson::bo keys = _owner->settings().shardKeysBson;
        int nFields = keys.nFields();
        _input.reset(new InputFormatJson(segment));
        _docLoc.location = _docLogicalLoc;
        _docLoc.start = _input->pos();
        //Reads in documents until the segment comes back with no more docs
        while (_input->next(&_doc)) {
            bson::bob extra;
            //TODO: Make sure that this extra field keys works with multikey indexes, sparse, etc
            //NULL is true so we can iterate a fixed distance.
            _docShardKey = _doc.extractFields(keys, false);
            _added_id = false;
            //Check to see if the document has a complete shard key
            if (_docShardKey.nFields() != nFields) {
                //If the shard key is only short by _id and we are willing to add it, do so
                //The shard key must be complete at this stage so all sorting is correct
                if (_owner->settings().indexHas_id && (nFields - _docShardKey.nFields()) == 1) {
                    _added_id = true;
                    auto oid = mongo::OID::gen();
                    extra.append("_id", oid);
                    if (nFields == 1) {
                        _docShardKey = BSON("_id" << oid);
                    }
                    else {
                        auto itr = _docShardKey.begin();
                        size_t pos = 0;
                        mongo::BSONObjBuilder index;
                        do {
                            if (pos == _owner->settings().indexPos_id) index.append(itr.next());
                            else index.append("_id", oid);
                            ++pos;
                        }
                        while (itr.more());
                        _docShardKey = index.obj();
                        assert(_docShardKey.nFields() == nFields);
                    }
                }
                else throw std::logic_error("No shard key in doc");
            }
            //TODO: Continue on error setting?
            assert(!_docShardKey.isEmpty());
            //If hashing is required, do it
            if (_owner->settings().hashed) _docShardKey =
                    BSON("_id-hash" << mongo::BSONElementHasher::hash64(_docShardKey.firstElement(), mongo::BSONElementHasher::DEFAULT_HASH_SEED));
            _extra = &extra;
            auto *stage = _queue.getStage(_docShardKey);
            stage->push(this);
            _docLoc.start = _input->pos();
        }
    }
} /* namespace loader */
