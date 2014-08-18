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

#ifndef BSONTOOLS_H_
#define BSONTOOLS_H_
#include "mongocxxdriver.h"

namespace cpp {

constexpr int MAX_ORDER_KEYS = 31;

inline std::ostream& operator<< (std::ostream &o, mongo::Ordering &rhs) {
    mongo::StringBuilder buf;
    for ( unsigned i=0; i < MAX_ORDER_KEYS; i++)
        buf.append( rhs.get(i) > 0 ? "+" : "-" );
    o << buf.str();
    return o;
}


class BSONObjCmp {
public:
    explicit BSONObjCmp(const bson::bo &o): _o(mongo::Ordering::make(o)) { }
    explicit BSONObjCmp(mongo::Ordering o): _o(std::move(o)) { }
    bool operator()(const bson::bo &l, const bson::bo &r) const {
        return l.woCompare(r, _o, false) < 0;
    }

    mongo::Ordering ordering() const { return _o; }

    operator std::string() const {
        mongo::StringBuilder buf;
        for ( unsigned i=0; i< MAX_ORDER_KEYS; i++)
            buf.append( _o.get(i) > 0 ? "+" : "-" );
       return buf.str();
    }

    friend std::ostream& operator<< (std::ostream &o, BSONObjCmp &rhs) {
        o << std::string(rhs);
        return o;
    }


private:
    const mongo::Ordering _o;
};

class BSONObjCmpDBG {
public:
    using KeyType = bson::bo;

    BSONObjCmpDBG(bson::bo o): _o(mongo::Ordering::make(o)) { }
    BSONObjCmpDBG(mongo::Ordering o): _o(std::move(o)) { }
    bool operator()(const bson::bo &l, const bson::bo &r) {
        std::cerr << l.jsonString(mongo::JsonStringFormat::TenGen, true, false) << "::" << r.jsonString(mongo::JsonStringFormat::TenGen, true, false) << std::endl;
        bool j = l.firstElement().Long() < r.firstElement().Long();
        bool v = l.woCompare(r, _o, false) < 0;
        if(j != v)
            assert(j == v);
        return v;
    }

    mongo::Ordering ordering() const { return _o; }

    friend std::ostream& operator<< (std::ostream &o, BSONObjCmpDBG &rhs) {
        //o << rhs.ordering();
        return o;
    }

private:
    const mongo::Ordering _o;
};

} /* namespace cpp */

#endif /* BSONTOOLS_H_ */
