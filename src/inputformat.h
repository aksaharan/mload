/*
 * inputformat.h
 *
 *  Created on: Aug 17, 2014
 *      Author: charlie
 */
#pragma once

#include <fstream>
#include "mongocxxdriver.h"
#include "tools.h"

namespace loader {

    /*
     * Public interface for the extraction of documents from sources.
     * bool next(mongo::BSONObj *nextDoc) is used to allow for the greatest variety of input sources
     */
    class InputFormat {
    public:
        virtual ~InputFormat() {
        }

        /**
         * If there is a document available, this function places the next one into the passed variable
         * @return returns true if there is document available. False otherwise.
         */
        virtual bool next(mongo::BSONObj *nextDoc) = 0;

        /**
         * Returns the position of the document.  Should assert if such a thing isn't possible and
         * this is called.
         */
        virtual size_t pos() = 0;
    };

    /**
     * Reads JSON from a file.
     */
    class InputFormatJson : public InputFormat {
    public:
        explicit InputFormatJson(const cpp::LocSegment &segment);
        virtual bool next(mongo::BSONObj *nextDoc);
        virtual size_t pos() {
            return _input.tellg();
        }
    private:
        std::ifstream _input;
        std::string _line;
    };

}  //namespace loader

