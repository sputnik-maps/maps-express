#include "cached_datasource.h"

CahedDataSource::CahedDataSource(mapnik::datasource_ptr ds)
    : datasource(mapnik::parameters()),
      p_datasource_(std::move(ds))
{
    assert(p_datasource_);
}

mapnik::datasource::datasource_t CahedDataSource::type() const {
    return p_datasource_->type();
}

boost::optional<mapnik::datasource_geometry_t> CahedDataSource::get_geometry_type() const {
    return p_datasource_->get_geometry_type();
}

mapnik::featureset_ptr CahedDataSource::features(mapnik::query const& q) const {
    const auto& q_bbox = q.get_bbox();
    if (q_bbox != cached_bbox_) {
        cached_bbox_ = q_bbox;
        cached_features_ = std::make_shared<CachedFeatureset>(p_datasource_->features(q));
    }
    // Copy made to ensure that returning member shared_ptr will not reset it
    return mapnik::featureset_ptr(cached_features_);
}

mapnik::featureset_ptr CahedDataSource::features_at_point(mapnik::coord2d const& pt, double tol) const {
    return p_datasource_->features_at_point(pt, tol);
}

mapnik::box2d<double> CahedDataSource::envelope() const {
    return p_datasource_->envelope();
}

mapnik::layer_descriptor CahedDataSource::get_descriptor() const {
    return p_datasource_->get_descriptor();
}
