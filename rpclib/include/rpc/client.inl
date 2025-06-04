#ifdef _WIN32
#include <windows.h>
#endif

#include "rpc/client_impl.h" // Only implementation files include this!
#include "rpc/detail/write_helper_fwd.h"

namespace rpc {

template <typename... Args>
RPCLIB_MSGPACK::object_handle client::call(std::string const &func_name, Args... args) {
    RPCLIB_CREATE_LOG_CHANNEL(client)
    auto future = async_call(func_name, std::forward<Args>(args)...);
    if (auto timeout = get_timeout()) {
        auto wait_result = future.wait_for(std::chrono::milliseconds(*timeout));
        if (wait_result == std::future_status::timeout) {
            throw_timeout(func_name);
        }
    }
    return future.get();
}

template <typename... Args>
std::future<RPCLIB_MSGPACK::object_handle>
client::async_call(std::string const &func_name, Args... args) {
    RPCLIB_CREATE_LOG_CHANNEL(client)
    wait_conn();
    using RPCLIB_MSGPACK::object;
    LOG_DEBUG("Calling {}", func_name);

    auto args_obj = std::make_tuple(args...);
    const int idx = get_next_call_idx();
    auto call_obj =
        std::make_tuple(static_cast<uint8_t>(client::request_type::call), idx,
                        func_name, args_obj);

    auto buffer = std::make_shared<RPCLIB_MSGPACK::sbuffer>();
    RPCLIB_MSGPACK::pack(*buffer, call_obj);

    auto p = std::make_shared<std::promise<RPCLIB_MSGPACK::object_handle>>();
    auto ft = p->get_future();

#ifdef _WIN32
    if (get_connection_type() == client::connection_type::NAMED_PIPE) {
        if (pimpl->pipe_handle_ != INVALID_HANDLE_VALUE) {
            ::rpc::detail::write_helper(pimpl->pipe_handle_, *buffer, true /* message_mode */);
            uint32_t resp_len = 0;
            DWORD read = 0;
            BOOL ok = ReadFile(pimpl->pipe_handle_, &resp_len, 4, &read, NULL);
            if (!ok || read != 4) {
                DWORD err = GetLastError();
                std::string err_msg = "[client::async_call] ReadFile (response length) failed, err=" + std::to_string(err);
                LOG_ERROR(err_msg);
                p->set_exception(std::make_exception_ptr(std::runtime_error(err_msg)));
                return ft;
            }
            if (resp_len == 0 || resp_len > (16 * 1024 * 1024)) {
                std::string err_msg = "[client::async_call] Invalid response frame size: " + std::to_string(resp_len);
                LOG_ERROR(err_msg);
                p->set_exception(std::make_exception_ptr(std::runtime_error(err_msg)));
                return ft;
            }
            std::vector<char> resp_buf(resp_len);
            ok = ReadFile(pimpl->pipe_handle_, resp_buf.data(), resp_len, &read, NULL);
            if (!ok || read != resp_len) {
                DWORD err = GetLastError();
                std::string err_msg = "[client::async_call] ReadFile (response payload) failed, err=" + std::to_string(err);
                LOG_ERROR(err_msg);
                p->set_exception(std::make_exception_ptr(std::runtime_error(err_msg)));
                return ft;
            }
            RPCLIB_MSGPACK::object_handle result;
            try {
                RPCLIB_MSGPACK::unpack(result, resp_buf.data(), resp_len);
                p->set_value(std::move(result));
            } catch (const std::exception& e) {
                p->set_exception(std::make_exception_ptr(e));
            }
        } else {
            std::string err_msg = "[client::async_call] Invalid pipe handle!";
            LOG_ERROR(err_msg);
            p->set_exception(std::make_exception_ptr(std::runtime_error(err_msg)));
        }
    } else {
        post(buffer, idx, func_name, p);
    }
#else
    post(buffer, idx, func_name, p);
#endif

    return ft;
}

template <typename... Args>
void client::send(std::string const &func_name, Args... args) {
    RPCLIB_CREATE_LOG_CHANNEL(client)
    LOG_DEBUG("Sending notification {}", func_name);

    auto args_obj = std::make_tuple(args...);
    auto call_obj = std::make_tuple(
        static_cast<uint8_t>(client::request_type::notification), func_name,
        args_obj);

    auto buffer = new RPCLIB_MSGPACK::sbuffer;
    RPCLIB_MSGPACK::pack(*buffer, call_obj);

#ifdef _WIN32
    if (get_connection_type() == client::connection_type::NAMED_PIPE) {
        if (pimpl->pipe_handle_ != INVALID_HANDLE_VALUE) {
            ::rpc::detail::write_helper(pimpl->pipe_handle_, *buffer, true /* message_mode */);
        } else {
            LOG_ERROR("[client::send] Invalid pipe handle!");
        }
        delete buffer;
    } else {
        post(buffer);
    }
#else
    post(buffer);
#endif
}

} // namespace rpc