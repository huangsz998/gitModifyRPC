#pragma once

#ifdef _WIN32
#include <windows.h>
#include "asio.hpp"
#include "rpc/detail/log.h"
#include <iostream>

namespace rpc {

inline bool safe_handle_assign(RPCLIB_ASIO::windows::stream_handle& stream, HANDLE handle) {
    try {
        if (handle == INVALID_HANDLE_VALUE) {
            LOG_ERROR("Cannot assign INVALID_HANDLE_VALUE");
            return false;
        }
        
        // 检查句柄是否有效
        DWORD flags = 0;
        if (!GetHandleInformation(handle, &flags)) {
            DWORD error = GetLastError();
            LOG_ERROR("GetHandleInformation failed with error {}", error);
            return false;
        }
        
        // 检查句柄类型
        DWORD type = GetFileType(handle);
        LOG_INFO("Handle type: {}", type);
        
        // 检查管道状态
        DWORD state = 0;
        DWORD instances = 0;
        DWORD max_collection = 0;
        char user_name[256] = {0};
        DWORD user_name_size = sizeof(user_name);
        
        if (GetNamedPipeHandleState(
                handle, &state, &instances, &max_collection, 
                nullptr, user_name, user_name_size)) {
            LOG_INFO("Pipe state: {}, instances: {}", state, instances);
        }
        
        // 执行分配
        stream.assign(handle);
        LOG_INFO("Successfully assigned handle to stream");
        return true;
    } 
    catch (const std::exception& e) {
        LOG_ERROR("Exception during handle assign: {}", e.what());
        return false;
    }
}

inline void log_win_error(const char* operation, DWORD error_code) {
    char* error_msg = nullptr;
    FormatMessageA(
        FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,
        NULL,
        error_code,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        (LPSTR)&error_msg,
        0,
        NULL
    );
    
    if (error_msg) {
        LOG_ERROR("{} failed: error {} - {}", operation, error_code, error_msg);
        std::cerr << operation << " failed: " << error_msg << std::endl;
        LocalFree(error_msg);
    } else {
        LOG_ERROR("{} failed: error {}", operation, error_code);
        std::cerr << operation << " failed with error code: " << error_code << std::endl;
    }
}

} // namespace rpc
#endif