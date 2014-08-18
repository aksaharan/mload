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

    InputProcessor::InputProcessor(Loader *owner, std::string ns):
        _owner(owner),
        _ns(ns),
        _queue(_owner->queueSettings(), owner->cluster(), &owner->opAgg(), ns),
        _docLogicalLoc{}  { }


    Bson InputProcessor::getFinalDoc() {
        if(_extra->iterator().more()) {
            if(_added_id) {
                _extra->appendElements(_doc);
                _doc = _extra->obj();
        }
        else
            //TODO: Handle the case were we aren't adding _id and it is in _doc, need to get _id first
            assert(_added_id);
        }
        return std::move(_doc);
    }

    Bson InputProcessor::getIndex() {
        return std::move(_docIndex);
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

    void InputProcessor::processSegmentToQueue(const cpp::LocSegment &segment, cpp::LogicalLoc logicalLoc) {
        _docLogicalLoc = logicalLoc;
        //May need to switch to void getFields(unsigned n, const char **fieldNames, BSONElement *fields) const;
        const bson::bo keys = _owner->settings().shardKeysBson;
        int nFields = keys.nFields();
        _input.reset(new InputFormatJson(segment));
        _docLoc.location = _docLogicalLoc;
        _docLoc.start = _input->pos();
        while(_input->next(&_doc)) {
            bson::bob extra;
            //TODO: Make sure that this extra field keys works with multikey indexes, sparse, etc
            //NULL is true so we can iterate a fixed distance.
            _docIndex = _doc.extractFields(keys, false);
            _added_id = false;
            if(_docIndex.nFields() != nFields) {
                if(_owner->settings().indexHas_id && (nFields - _docIndex.nFields()) == 1) {
                    _added_id = true;
                    auto oid = mongo::OID::gen();
                    extra.append("_id", oid);
                    if(nFields == 1) {
                        _docIndex = BSON("_id" << oid);
                    }
                    else {
                        auto itr = _docIndex.begin();
                        size_t pos = 0;
                        mongo::BSONObjBuilder index;
                        do {
                            if(pos == _owner->settings().indexPos_id)
                                index.append(itr.next());
                            else
                                index.append("_id", oid);
                            ++pos;
                        } while (itr.more());
                        _docIndex = index.obj();
                        assert(_docIndex.nFields() == nFields);
                    }
                }
                else
                    throw std::logic_error("No shard key in doc");
            }
            //TODO: make this cleaner, continue on error setting?
            assert(!_docIndex.isEmpty());
            if(_owner->settings().hashed)
                _docIndex = BSON("_id-hash" << mongo::BSONElementHasher::hash64(_docIndex.firstElement(), mongo::BSONElementHasher::DEFAULT_HASH_SEED));
            _extra = &extra;
            auto *stage = _queue.getStage(_docIndex);
            stage->push(this);
            _docLoc.start = _input->pos();
        }
    }
} /* namespace loader */
