/*
 * inputformat.h
 *
 *  Created on: Aug 17, 2014
 *      Author: charlie
 */

#ifndef INPUTFORMAT_H_
#define INPUTFORMAT_H_

#include <fstream>
#include "mongocxxdriver.h"
#include "tools.h"

namespace loader {

    /*
     * This class connections to an concrete input source.
     * bool next(mongo::BSONObj *nextDoc) is used to allow for the greatest variety of input sources
     */
    class InputFormat {
    public:
        virtual ~InputFormat() {};
        virtual bool next(mongo::BSONObj *nextDoc) = 0;
        virtual size_t pos() = 0;
    };

    class InputFormatJson : public InputFormat {
    public:
        explicit InputFormatJson(const cpp::LocSegment &segment);
        virtual bool next(mongo::BSONObj *nextDoc);
        virtual size_t pos() { return _input.tellg(); }
    private:
        std::ifstream _input;
        std::string _line;
    };

} /* namespace loader */

#endif /* INPUTFORMAT_H_ */
