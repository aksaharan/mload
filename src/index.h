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

#ifndef INDEX_H_
#define INDEX_H_

#include <algorithm>
#include <assert.h>
#include <deque>
#include <tuple>
#include <utility>

namespace cpp {


    template<typename Cmp, typename Key>
    struct IndexPairCompare {
        IndexPairCompare(Cmp cmp): compare(std::move(cmp)) { }

        template <typename IndexDataType>
        bool operator()(const IndexDataType &l, const IndexDataType &r) const {
            static_assert(std::is_same<typename IndexDataType::first_type, Key>::value, "Need to have the same key types to compare");
            return compare(l.first, r.first);
        }

        template <typename IndexDataType>
        bool operator()(const Key &l, const IndexDataType &r) const {
            static_assert(std::is_same<typename IndexDataType::first_type, Key>::value, "Need to have the same key types to compare");
            return compare(l, r.first);
        }

        template <typename IndexDataType>
        bool operator()(const IndexDataType &l, const Key &r) const {
            static_assert(std::is_same<typename IndexDataType::first_type, Key>::value, "Need to have the same key types to compare");
            return compare(l.first, r);
        }

        friend std::ostream& operator<< (std::ostream &o, const IndexPairCompare &c) {
            o << c.compare;
            return o;
        }

        //NOT const.  The container can declare this class const if so desired
        Cmp compare;
    };

    /*
     * Key = index
     * Tll = logical location
     * Compare = bool operator()(Key, Key) for sorting.
     */
    template <typename Key, typename Value, typename Compare>
    class BasicIndex {
    public:
        using Data = typename std::pair<Key, Value>;
        using Container = typename std::deque<Data>;
        using iterator = typename Container::iterator;
        using const_iterator = typename Container::const_iterator;
        using CompareHolder = IndexPairCompare<Compare, Key>;

        BasicIndex(BasicIndex &&rhs) :
            _compare(std::move(rhs._compare)),
            _container(std::move(rhs._container)) { }
        template <typename ...Args>
        BasicIndex(const BasicIndex &bi, Args&&... args) :
        _compare(CompareHolder(Cmp(std::forward<Args>(args)...))),
        _container(bi) {}
        template <typename ...Args>
        BasicIndex(Args... args) : _compare(CompareHolder(Compare(args...))) {}
        explicit BasicIndex(Compare compare): _compare(CompareHolder(std::move(compare))) { }

        friend std::ostream& operator<<(std::ostream &o, const BasicIndex::iterator &i) {
            o << i->first << "::" << i->second;
            return o;
        }

        //TODO: make begin and end work
        const_iterator cbegin() const { return _container.cbegin(); }
        const_iterator cend() const { return _container.cend(); }
        iterator begin() { return _container.begin(); }
        iterator end() { return _container.end(); }
        Value& front() { return _container.front().second; }
        Value& back() { return _container.back().second; }
        void clear() { _container.clear(); }
        void sort() { std::sort(_container.begin(), _container.end(), _compare); }
        size_t size() const { return _container.size(); }
        Container& container() { return _container; }

        Compare getCompare() const { return _compare.compare; }

        void insertUnordered(Key index, Value value) {
            _container.emplace_back(std::make_pair(std::move(index), std::move(value)));
        }

        template <typename InputIterator>
        void insertUnordered(InputIterator first, InputIterator last) {
            static_assert(std::is_constructible<typename std::iterator_traits<InputIterator>::value_type, Data>::value, "Cannot construct insert type");
            _container.insert(end(), first, last);
        }

        void steal(BasicIndex &takeFrom) {
            insertUnordered(takeFrom.begin(), takeFrom.end());
            takeFrom.clear();
        }

        void finalize() {
            sort();
        }

        iterator find(const Key &key) {
            auto i = std::lower_bound(begin(), end(), key, _compare);
            //TODO: remove assert, right now it's here because this should never be true
            assert(i->first == key);
            //Ensure positive infinity returns the correct result
            return i;

        }

        Value& at(const Key &key) {
            auto i = find(key);
            if(i == end())
                throw std::range_error("Index out of bounds");
            return i->second;
        }

        bool empty() { return _container.empty(); }

        /*
         * Assumes upper bounded includes infinity, so nothing is ever returned at end.
         */
        Value& upperBoundSafe(const Key &key) {
            auto i = std::upper_bound(begin(), end(), key, _compare);
            //Ensure positive infinity returns the correct result
            if(i == _container.end()) --i;
            return i->second;
        }

        Value& upperBound(const Key &key) {
            auto i = std::upper_bound(begin(), end(), key, _compare);
            //TODO: remove assert, right now this should never be true so it's here
            assert(i != end());
            return i->second;
        }

        Value& lowerBound(const Key &key) {
            auto i = std::lower_bound(begin(), end(), key, _compare);
            return i->second;
        }

        friend std::ostream& operator<< (std::ostream & o, const BasicIndex &rhs) {
            for(auto &i: rhs._container) {
                o << i->first << "\n";
            }
            return o;
        }

    private:

        CompareHolder _compare;
        Container _container;
    };

    template <typename Key, typename Value, typename Cmp = std::less<Key>> using Index = BasicIndex<Key, Value, Cmp>;

} /* namespace cpp */

#endif /* INDEX_H_ */