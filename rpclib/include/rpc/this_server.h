#pragma once

#ifndef THIS_SERVER_H_X0CJLVVW
#define THIS_SERVER_H_X0CJLVVW

namespace rpc {

namespace detail {
class server_session;
template<typename SocketType>
class server_pipe_session;
}

//! \brief Allows controlling the server instance from the
//! currently executing handler.
class this_server_t {
public:
    //! \brief Gracefully stops the server.
    void stop();

    //! \brief Cancels a requested stop operation.
    void cancel_stop();

    //! Check if a stop is requested
    bool stopping() const { return stopping_; }

private:
    friend class rpc::detail::server_session;
    // 添加模板类server_pipe_session为友元，以允许它访问私有成员
    template<typename SocketType>
    friend class rpc::detail::server_pipe_session;
    
    bool stopping_;
};

//! \brief A thread-local object that can be used to control
//! the behavior of the server w.r.t. the handler. Accessing this object
//! from handlers that execute the same function concurrently is safe.
//! \note Accessing this object outside of handlers while a server is
//! running is potentially unsafe.
this_server_t &this_server();

} /* rpc */

#endif /* end of include guard: THIS_SERVER_H_X0CJLVVW */