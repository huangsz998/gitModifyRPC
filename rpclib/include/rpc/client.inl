namespace rpc {

    template <typename... Args>
    RPCLIB_MSGPACK::object_handle client::call(std::string const &func_name,
                                        Args... args) {
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
        // On Windows, if using Named Pipe, force message_mode/framing write (framing by async_writer now)
        if (get_connection_type() == client::connection_type::NAMED_PIPE) {
            // Write directly using post, which is implemented to do framing
            post(buffer, idx, func_name, p);
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
        // On Windows, if using Named Pipe, force message_mode/framing write (framing by async_writer now)
        if (get_connection_type() == client::connection_type::NAMED_PIPE) {
            post(buffer);
        } else {
            post(buffer);
        }
    #else
        post(buffer);
    #endif
    }
    
    }