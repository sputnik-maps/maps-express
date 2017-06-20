#pragma once

#include <experimental/optional>

#include <jsoncpp/json/value.h>

#include <glog/logging.h>


namespace json_util {

namespace detail {

template <typename T>
bool validate(const Json::Value& jvalue);

template <>
inline bool validate<std::string>(const Json::Value& jvalue) {
    return jvalue.isString();
}

template <>
inline bool validate<int>(const Json::Value& jvalue) {
    return jvalue.isInt();
}

template <>
inline bool validate<std::int64_t>(const Json::Value& jvalue) {
    return jvalue.isInt64();
}

template <>
inline bool validate<uint>(const Json::Value& jvalue) {
    return jvalue.isUInt();
}

template <>
inline bool validate<std::uint64_t>(const Json::Value& jvalue) {
    return jvalue.isUInt64();
}


template <>
inline bool validate<double>(const Json::Value& jvalue) {
    return jvalue.isDouble();
}

template <>
inline bool validate<bool>(const Json::Value& jvalue) {
    return jvalue.isBool();
}

//template <typename T>
//bool validate<std::vector<T>>(const Json::Value& jvalue) {
//    if (!jvalue.isArray()) {
//        return false;
//    }
//    for (const Json::Value& jitem : jvalue) {
//        if (!validate<T>(jitem)) {
//            return false;
//        }
//    }
//    return true;
//}


template <typename T>
T from_json(const Json::Value& jvalue);

template <>
inline std::string from_json<std::string>(const Json::Value& jvalue) {
    return jvalue.asString();
}

template <>
inline int from_json<int>(const Json::Value& jvalue) {
    return jvalue.asInt();
}

template <>
inline std::int64_t from_json<std::int64_t>(const Json::Value& jvalue) {
    return jvalue.asInt64();
}

template <>
inline uint from_json<uint>(const Json::Value& jvalue) {
    return jvalue.asUInt();
}

template <>
inline std::uint64_t from_json<std::uint64_t>(const Json::Value& jvalue) {
    return jvalue.asUInt64();
}

template <>
inline double from_json<double>(const Json::Value& jvalue) {
    return jvalue.asDouble();
}

template <>
inline bool from_json<bool>(const Json::Value& jvalue) {
    return jvalue.asBool();
}

//template <typename T>
//std::vector<T> from_json<std::vector<T>>(const Json::Value& jvalue) {
//    std::vector<T> result;
//    result.reserve(jvalue.size());
//    for (const Json::Value& jitem : jvalue) {
//        result.push_back(from_json<T>(jitem));
//    }
//    return result;
//}

} // ns detail

using std::experimental::optional;
using std::experimental::nullopt;

template <typename T>
inline T FromJson(const Json::Value& jvalue, const T& default_value) {
    if (!detail::validate<T>(jvalue)) {
        return default_value;
    }
    return detail::from_json<T>(jvalue);
}


template <typename T>
inline optional<T> FromJsonOrErr(const Json::Value& jvalue, const std::string& err_string) {
    if (!detail::validate<T>(jvalue)) {
        LOG(ERROR) << err_string;
        return nullopt;
    }
    return detail::from_json<T>(jvalue);
}

} // ns json_util
