#pragma once

#ifdef _WIN32
#include <windows.h>
#endif
#include <rpc/msgpack.hpp>

namespace rpc {
namespace detail {

// Forward declaration for synchronous pipe write_helper
inline void write_helper(
    #ifdef _WIN32
    HANDLE hPipe,
    #else
    int hPipe, // fallback for non-windows, won't be used
    #endif
    RPCLIB_MSGPACK::sbuffer &buf,
    bool message_mode);

} // namespace detail
} // namespace rpc