#pragma once

#include <vector>

#include <mapnik/featureset.hpp>

class CachedFeatureset : public mapnik::Featureset {
public:
    CachedFeatureset(mapnik::featureset_ptr fs);

    mapnik::feature_ptr next() override;

private:
    using cached_features_t = std::vector<mapnik::feature_ptr>;
    cached_features_t cached_features_;
    const mapnik::featureset_ptr fs_;
    cached_features_t::iterator iter_;
    bool cached_{false};
};
