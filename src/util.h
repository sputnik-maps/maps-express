#pragma once

#include <cmath>
#include <memory>
#include <iostream>
#include <string>
#include <set>
#include <type_traits>
#include <vector>


#define ME_PRINT_TID \
    std::cout << __PRETTY_FUNCTION__ << " called on thread " << std::this_thread::get_id() << std::endl;

namespace util {

void decompress(const char *data, size_t data_size, std::string& uncomp);

inline void decompress(const std::string data, std::string& uncomp) {
    decompress(data.data(), data.size(), uncomp);
}

template<typename T, typename ...Args>
std::unique_ptr<T> make_unique( Args&& ...args )
{
    return std::unique_ptr<T>( new T( std::forward<Args>(args)... ) );
}

namespace detail {

inline void Append(std::string& container, std::string&& val) {
    container.append(std::move(val));
}

inline void Append(std::vector<std::string>& container, std::string&& val) {
    container.push_back(std::move(val));
}

inline void Append(std::set<std::string>& container, std::string&& val) {
    container.insert(std::move(val));
}

} // ns detail


template <typename ContainerType>
inline void split(const std::string& data, ContainerType& container, const std::string& delimiter = "/") {
    static_assert(std::is_same<ContainerType, std::string>::value ||
                  std::is_same<ContainerType, std::vector<std::string>>::value ||
                  std::is_same<ContainerType, std::set<std::string>>::value,
                  "container should be one of the folowing types: std::string, std::vector<std::string>, std::set<std::string>");

    size_t data_len = data.length();
    if (data_len == 0) {
        return;
    }
    size_t delim_len = delimiter.length();
    size_t stop_pos, start_pos = 0;
    while ((stop_pos = data.find(delimiter, start_pos)) != std::string::npos) {
        if (stop_pos != start_pos) {
            detail::Append(container, std::string(data, start_pos, stop_pos - start_pos));
        }
        start_pos = stop_pos + delim_len;
        if (start_pos >= data_len) {
            return;
        }
    }
    detail::Append(container, std::string(data, start_pos, stop_pos - start_pos));
}

std::unique_ptr<std::set<std::string>> ParseArray(const std::string& layers);

inline constexpr double zoom_to_scale_denominator(uint zoom) {
    return 559082264 / std::pow(2, zoom);
}

inline constexpr double scale_denominator_to_zoom(double scale_denominator) {
    return std::log2(559082264 / scale_denominator);
}

inline constexpr const char* http_status_msg(uint16_t http_code) {
    switch (http_code) {
      case 100: return "Continue";
      case 101: return "Switching Protocols";
      case 200: return "OK";
      case 201: return "Created";
      case 202: return "Accepted";
      case 203: return "Non-Authoritative Information";
      case 204: return "No Content";
      case 205: return "Reset Content";
      case 206: return "Partial Content";
      case 300: return "Multiple Choices";
      case 301: return "Moved Permanently";
      case 302: return "Found";
      case 303: return "See Other";
      case 304: return "Not Modified";
      case 305: return "Use Proxy";
      case 307: return "Temporary Redirect";
      case 400: return "Bad Request";
      case 401: return "Unauthorized";
      case 402: return "Payment Required";
      case 403: return "Forbidden";
      case 404: return "Not Found";
      case 405: return "Method Not Allowed";
      case 406: return "Not Acceptable";
      case 407: return "Proxy Authentication Required";
      case 408: return "Request Timeout";
      case 409: return "Conflict";
      case 410: return "Gone";
      case 411: return "Length Required";
      case 412: return "Precondition Failed";
      case 413: return "Request Entity Too Large";
      case 414: return "Request-URI Too Long";
      case 415: return "Unsupported Media Type";
      case 416: return "Requested Range Not Satisfiable";
      case 417: return "Expectation Failed";
      case 418: return "I'm a teapot";
      case 500: return "Internal Server Error";
      case 501: return "Not Implemented";
      case 502: return "Bad Gateway";
      case 503: return "Service Unavailable";
      case 504: return "Gateway Timeout";
      case 505: return "HTTP Version Not Supported";
    }
    return "-";
}


enum class ExtensionType {
    none,
    png,
    mvt,
    json,
    html,
};

ExtensionType str2ext(std::string& ext);
std::string ext2str(ExtensionType ext);

} // ns util
