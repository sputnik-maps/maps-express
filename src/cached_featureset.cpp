#include "cached_featureset.h"

#include <cassert>

CachedFeatureset::CachedFeatureset(mapnik::featureset_ptr fs)
    : fs_(std::move(fs)),
      cached_(false)
{
    assert(fs_);
}

mapnik::feature_ptr CachedFeatureset::next() {
    if (cached_) {
        if (iter_ == cached_features_.end()) {
            iter_ = cached_features_.begin();
            return mapnik::feature_ptr();
        }
        return mapnik::feature_ptr(*iter_++);
    }
    auto f_ptr = fs_->next();
    if (f_ptr) {
        cached_features_.push_back(f_ptr);
    } else{
        cached_ = true;
        iter_ = cached_features_.begin();
    }
    return f_ptr;
}
