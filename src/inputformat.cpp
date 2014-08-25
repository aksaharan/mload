/*
 * inputformat.cpp
 *
 *  Created on: Aug 17, 2014
 *      Author: charlie
 */

#include "inputformat.h"
#include <assert.h>

namespace loader {

    InputFormatJson::InputFormatJson(const cpp::LocSegment &segment) {
        _input.open(segment, std::ios_base::in);
        assert(_input.is_open());
    }

    //TODO: Keep average object size and implement fromjson with a large/smaller buffer
    bool InputFormatJson::next(mongo::BSONObj *nextDoc) {
        bool next = getline(_input, _line);
        if (!next) return next;
        *nextDoc = mongo::fromjson(_line);
        return next;
    }

}  //namespace loader
