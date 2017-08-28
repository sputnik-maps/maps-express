#pragma once

#include <list>
#include <unordered_map>

#include <experimental/optional>

template <typename T>
class LRUCache {
public:
    LRUCache(std::size_t capacity) : capacity_(capacity) {}

    bool Set(const std::string& key, T value) {
        auto item_map_itr = items_map_.find(key);
        if (item_map_itr == items_map_.end()) {
            items_.emplace_back(key, std::move(value));
            items_map_[key] = --items_.end();
            if (items_.size() > capacity_) {
                item_t& first_item = items_.front();
                items_map_.erase(first_item.first);
                items_.pop_front();
            }
            return true;
        }

        auto item_itr = item_map_itr->second;
        item_itr->second = std::move(value);
        items_.splice(item_itr, items_, items_.end());
        return false;
    }

    std::experimental::optional<T> Get(const std::string& key) {
        auto item_map_itr = items_map_.find(key);
        if (item_map_itr == items_map_.end()) {
            return std::experimental::nullopt;
        }
        auto item_itr = item_map_itr->second;
        items_.splice(item_itr, items_, items_.end());
        return item_itr->second;
    }

    bool Remove(const std::string& key) {
        auto item_map_itr = items_map_.find(key);
        if (item_map_itr == items_map_.end()) {
            return false;
        }
        items_.erase(item_map_itr->second);
        items_map_.erase(item_map_itr);
        return true;
    }

    void Clear() {
        items_.clear();
        items_map_.clear();
    }

private:
    using item_t = std::pair<std::string, T>;
    using items_list_t = std::list<item_t>;

    items_list_t items_;
    std::unordered_map<std::string, typename items_list_t::iterator> items_map_;
    std::size_t capacity_;
};
