#pragma once

#include <mapnik/datasource.hpp>

#include "cached_featureset.h"

class CahedDataSource : public mapnik::datasource {
public:
    CahedDataSource(mapnik::datasource_ptr ds);

    datasource_t type() const override;
    boost::optional<mapnik::datasource_geometry_t> get_geometry_type() const override;
    mapnik::featureset_ptr features(mapnik::query const& q) const override;
    mapnik::featureset_ptr features_at_point(mapnik::coord2d const& pt, double tol = 0) const override;
    mapnik::box2d<double> envelope() const override;
    mapnik::layer_descriptor get_descriptor() const override;

private:
    const mapnik::datasource_ptr p_datasource_;
    mutable mapnik::box2d<double> cached_bbox_;
    mutable mapnik::featureset_ptr cached_features_{nullptr};
};
