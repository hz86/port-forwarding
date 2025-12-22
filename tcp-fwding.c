/*
 简短转发流程：

 1. 初始化（pf_tcp_create）
    - 初始化 Winsock、解析 IP 地址、创建 IOCP、定时器队列。
    - 创建会话池和会话表以管理连接资源。

 2. 启动（pf_tcp_start）
    - 创建监听 Socket，动态加载 AcceptEx/ConnectEx/DisconnectEx 等扩展函数。
    - 绑定监听端口并关联 IOCP，启动工作线程池和守护线程。

 3. 接受连接（Daemon Thread / post_accept）
    - 守护线程监控 active_accepts 数量，不足时循环调用 post_accept。
    - post_accept 从池中获取会话，通过 AcceptEx 投递异步接受请求。

 4. Accept 完成（handle_accept）
    - 调用 SO_UPDATE_ACCEPT_CONTEXT 更新上下文，解析客户端地址。
    - 如配置超时则 post_timeout，然后调用 post_connect 对目标发起 ConnectEx。

 5. Connect 完成（handle_connect）
    - 取消连接超时计时器（session_cancel_timeout）。
    - 调用 SO_UPDATE_CONNECT_CONTEXT，获取目标两端地址。
    - 同时向源端和目标端投递异步接收请求（post_recv），进入双向转发状态。

 6. 数据转发（读/写循环）
    - 链式数据流：
      - 源读 -> 目标写 -> 源读
      - 目标读 -> 源写 -> 目标读
    - 读完成（handle_*_read）：
      - 若 bytes > 0：将数据投递给对端发送（post_send）。
      - 若 bytes == 0：标记半关闭，投递 DisconnectEx（post_disconnect）以复用 Socket。
    - 写完成（handle_*_write）：
      - 若发送未发完：调整缓冲区偏移继续发送。
      - 若发送完毕：重新向数据来源端投递接收请求（post_recv）。
    - Disconnect 完成：根据成功与否请求关闭会话，准备回收。

 7. 超时与关闭资源管理
    - 定时器回调 handle_timer_callback 通过 IOCP 通知（区分 bytes=1 到期和 bytes=0 取消）
      超时或错误时调用 session_request_close，最终由 session_io_unlock / session_release 回收资源。

 8. 停止与清理（pf_tcp_stop / 线程退出）
    - 停止守护线程，向 IOCP 投递退出信号（completion_key == 0）。
    - 工作线程逐个退出，最后一个退出的工作线程负责全局清理：
      - 关闭监听 Socket、遍历会话表强制关闭所有残留会话、排空 IOCP 队列并释放内存。

 关键技术点：
 - IOCP 模型：全异步事件驱动（Accept/Connect/Read/Write/Disconnect/Timeout）。
 - 内存管理：会话池（Pool）与会话表（Table）结合，减少内存分配开销。
 - 锁机制：使用 Interlocked 原子操作与自旋锁（session_io_lock/unlock）保证多线程安全。
*/

#define _CRT_SECURE_NO_WARNINGS

#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include <winsock2.h>
#include <ws2tcpip.h>
#include <mswsock.h>
#include <windows.h>

#include "pool.h"
#include "table.h"
#include "fwding.h"

#pragma comment(lib, "ws2_32.lib")

// IO操作类型
typedef enum {
    PF_IO_NONE = 0,
    PF_IO_ACCEPT = 1,
    PF_IO_CONNECT = 2,
    PF_IO_SRC_READ = 3,
    PF_IO_SRC_WRITE = 4,
    PF_IO_SRC_DISCONNECT = 5,
    PF_IO_DST_READ = 6,
    PF_IO_DST_WRITE = 7,
    PF_IO_DST_DISCONNECT = 8,
    PF_IO_TIMEOUT = 9,
} pf_io_type_t;

// 会话关闭类型
typedef enum {
    PF_SESSION_NONE = 0,
    PF_SESSION_CLOSE = 1,
    PF_SESSION_CLOSE_END = 2,
    PF_SESSION_SRC_CLOSE = 4,
    PF_SESSION_SRC_CLOSE_END = 8,
    PF_SESSION_DST_CLOSE = 16,
    PF_SESSION_DST_CLOSE_END = 32,
} pf_session_close_flag_t;

// 会话超时类型
typedef enum {
    PF_SESSION_TIMEOUT_NONE = 0,
    PF_SESSION_TIMEOUT_START = 1,
    PF_SESSION_TIMEOUT_COMPLETE = 2,
    PF_SESSION_TIMEOUT_END = 3,
} pf_session_timeout_type_t;

// 联合地址结构
typedef union pf_sockaddr_t {
    struct sockaddr addr;
    struct sockaddr_in sin4;
    struct sockaddr_in6 sin6;
} pf_sockaddr_t;

// 转发数据结构
typedef struct pf_tcp_t {
    // 运行状态
    volatile LONG running;        // 是否运行中
    volatile LONG active_threads; // 运行的线程数
    volatile LONG active_accepts; // 运作的接受数量
    volatile LONG post_count;     // 投递数量

    // 工作线程
    HANDLE stop_event;            // 停止事件
    HANDLE thread_daemon;         // 守护线程
    HANDLE* thread_workers;       // 工作线程
    int total_accepts;            // 总接受数量
    int thread_count;             // 工作线程数量

    // 配置信息
    pf_config_t config;           // 转发信息
    pf_sockaddr_t src_addr;       // 地址数据
    pf_sockaddr_t out_addr;       // 地址数据
    pf_sockaddr_t dst_addr;       // 地址数据

    // 套接字相关
    HANDLE iocp;                    // IOCP句柄
    HANDLE timer_queue;             // 定时器队列
    pool_t* session_pool;           // 会话池
    table_t* session_table;         // 会话表
    SOCKET listen_sock;             // 监听套接字
    LPFN_ACCEPTEX acceptex;         // 扩展接口
    LPFN_CONNECTEX connectex;       // 扩展接口
    LPFN_DISCONNECTEX disconnectex; // 扩展接口
    LPFN_GETACCEPTEXSOCKADDRS get_accept_sockaddrs;
} pf_tcp_t;

// 重叠IO上下文
typedef struct pf_session_t pf_session_t;
typedef struct pf_io_context_t {
    OVERLAPPED overlapped;        // 重叠结构
    pf_io_type_t operation_type;  // 操作类型
    pf_session_t* session;        // 会话指针
} pf_io_context_t;

// 会话上下文
typedef struct pf_session_t {
    volatile LONG ref;            // 引用计数
    volatile LONG lock;           // 会话自旋锁
    volatile LONG close_flags;    // 关闭标志
    volatile LONG timeout_status; // 超时状态

    ssize_t session_id;           // 唯一会话ID
    HANDLE timeout_timer;         // 定时器句柄
    pf_tcp_t* pf;                 // 父指针

    SOCKET src_sock;              // 源套接字
    SOCKADDR* src_local_addr;     // 源本地地址
    SOCKADDR* src_remote_addr;    // 源远程地址
    WSABUF src_wsa_buffer;        // 源缓冲区

    SOCKET dst_sock;              // 目标套接字
    SOCKADDR* dst_local_addr;     // 目标本地地址
    SOCKADDR* dst_remote_addr;    // 目标远程地址
    WSABUF dst_wsa_buffer;        // 目标缓冲区

    // 重叠IO上下文
    pf_io_context_t src_io;       // 源IO上下文
    pf_io_context_t dst_io;       // 目标IO上下文
    pf_io_context_t tmo_io;       // 超时IO上下文

    // 缓冲区
    char src_addr_buffer[(sizeof(pf_sockaddr_t) + 16) * 2];
    char dst_addr_buffer[(sizeof(pf_sockaddr_t) + 16) * 2];
    char src_data_buffer[8192];
    char dst_data_buffer[8192];
} pf_session_t;

// 内部函数声明
static int post_accept(pf_tcp_t* pf);
static int post_connect(pf_tcp_t* pf, pf_session_t* session);
static int post_timeout(pf_tcp_t* pf, pf_session_t* session);
static int post_disconnect(pf_tcp_t* pf, pf_session_t* session, pf_io_type_t type);
static int post_recv(pf_tcp_t* pf, pf_session_t* session, pf_io_type_t type);
static int post_send(pf_tcp_t* pf, pf_session_t* session, pf_io_type_t type, int bytes_transferred, int offset);
static int parse_ip_addr(const char* ip, unsigned short port, pf_sockaddr_t* addr);
static int get_sockaddr_len(pf_sockaddr_t* sockaddr);
static pool_t* session_pool_create(pf_tcp_t* pf, size_t max_size);
static void session_pool_close(pool_t* pool);
static pf_session_t* session_create(pf_tcp_t* pf);
static void session_ref(pf_session_t* session);
static void session_io_lock(pf_session_t* session);
static void session_io_unlock(pf_session_t* session);
static void session_request_close(pf_session_t* session, pf_session_close_flag_t flag);
static void session_cancel_timeout(pf_session_t* session);
static void session_release(pf_session_t* session);
static DWORD WINAPI thread_daemon_callback(LPVOID param);
static DWORD WINAPI thread_worker_callback(LPVOID param);

// TCP转发创建
pf_tcp_t* pf_tcp_create(pf_config_t* config)
{
    pf_tcp_t* pf;
    WSADATA wsa_data;
    if (0 != WSAStartup(MAKEWORD(2, 2), &wsa_data))
    {
        goto ERROR_1;
    }

    pf = (pf_tcp_t*)malloc(sizeof(pf_tcp_t));
    if (NULL == pf)
    {
        goto ERROR_2;
    }

    memset(pf, 0, sizeof(pf_tcp_t));
    memcpy(&pf->config, config, sizeof(pf_config_t));

    SYSTEM_INFO sys_info;
    GetSystemInfo(&sys_info);
    pf->thread_count = sys_info.dwNumberOfProcessors * 2;
    pf->thread_workers = (HANDLE*)malloc(sizeof(HANDLE) * pf->thread_count);
    if (NULL == pf->thread_workers)
    {
        goto ERROR_3;
    }

    if (-1 == parse_ip_addr(pf->config.src_ip, pf->config.src_port, &pf->src_addr))
    {
        goto ERROR_4;
    }

    if (-1 == parse_ip_addr(pf->config.dst_ip, pf->config.dst_port, &pf->dst_addr))
    {
        goto ERROR_4;
    }

    if (0 == pf->config.out_ip[0])
    {
        if (AF_INET == pf->dst_addr.addr.sa_family)
        {
            strcpy(pf->config.out_ip, "0.0.0.0");
        }
        else if (AF_INET6 == pf->dst_addr.addr.sa_family)
        {
            strcpy(pf->config.out_ip, "::");
        }
    }

    if (-1 == parse_ip_addr(pf->config.out_ip, 0, &pf->out_addr))
    {
        goto ERROR_4;
    }

    pf->timer_queue = CreateTimerQueue();
    if (NULL == pf->timer_queue)
    {
        goto ERROR_4;
    }

    pf->session_pool = session_pool_create(pf, 4096);
    if (NULL == pf->session_pool)
    {
        goto ERROR_5;
    }

    pf->session_table = table_create(16, 8192);
    if (NULL == pf->session_table)
    {
        goto ERROR_6;
    }

    pf->iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
    if (NULL == pf->iocp)
    {
        goto ERROR_7;
    }

    pf->running = 0;
    pf->listen_sock = INVALID_SOCKET;
    pf->total_accepts = pf->thread_count * 2;
    if (pf->total_accepts < 32)
    {
        pf->total_accepts = 32;
    }

    return pf;

ERROR_7:
    table_close(pf->session_table);
ERROR_6:
    session_pool_close(pf->session_pool);
ERROR_5:
    (void)DeleteTimerQueueEx(pf->timer_queue, INVALID_HANDLE_VALUE);
ERROR_4:
    free(pf->thread_workers);
ERROR_3:
    free(pf);
ERROR_2:
    WSACleanup();
ERROR_1:
    return NULL;
}

// TCP转发销毁
void pf_tcp_destroy(pf_tcp_t* pf)
{
    if (NULL == pf)
    {
        return;
    }

    if (0 != pf->running)
    {
        pf_tcp_stop(pf);
    }

    CloseHandle(pf->iocp);
    table_close(pf->session_table);
    session_pool_close(pf->session_pool);
    (void)DeleteTimerQueueEx(pf->timer_queue, INVALID_HANDLE_VALUE);
    free(pf->thread_workers);
    free(pf);

    WSACleanup();
}

// TCP转发启动
int pf_tcp_start(pf_tcp_t* pf)
{
    DWORD bytes = 0;
    GUID guid_acceptex = WSAID_ACCEPTEX;
    GUID guid_connectex = WSAID_CONNECTEX;
    GUID guid_disconnectex = WSAID_DISCONNECTEX;
    GUID guid_get_acceptex_sockaddrs = WSAID_GETACCEPTEXSOCKADDRS;

    if (NULL == pf || 0 != pf->running)
    {
        goto ERROR_1;
    }

    // 创建停止事件
    pf->stop_event = CreateEventW(NULL, TRUE, FALSE, NULL);
    if (NULL == pf->stop_event)
    {
        goto ERROR_1;
    }

    // 创建TCP监听套接字
    pf->listen_sock = WSASocketW(pf->src_addr.addr.sa_family,
        SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
    if (INVALID_SOCKET == pf->listen_sock)
    {
        goto ERROR_2;
    }

    // 获取扩展函数
    if (SOCKET_ERROR == WSAIoctl(pf->listen_sock, SIO_GET_EXTENSION_FUNCTION_POINTER,
        &guid_acceptex, sizeof(GUID), &pf->acceptex, sizeof(FARPROC), &bytes, NULL, NULL))
    {
        goto ERROR_3;
    }

    // 获取扩展函数
    if (SOCKET_ERROR == WSAIoctl(pf->listen_sock, SIO_GET_EXTENSION_FUNCTION_POINTER,
        &guid_connectex, sizeof(GUID), &pf->connectex, sizeof(FARPROC), &bytes, NULL, NULL))
    {
        goto ERROR_3;
    }

    // 获取扩展函数
    if (SOCKET_ERROR == WSAIoctl(pf->listen_sock, SIO_GET_EXTENSION_FUNCTION_POINTER,
        &guid_disconnectex, sizeof(GUID), &pf->disconnectex, sizeof(FARPROC), &bytes, NULL, NULL))
    {
        goto ERROR_3;
    }

    // 获取扩展函数
    if (SOCKET_ERROR == WSAIoctl(pf->listen_sock, SIO_GET_EXTENSION_FUNCTION_POINTER,
        &guid_get_acceptex_sockaddrs, sizeof(GUID), &pf->get_accept_sockaddrs, sizeof(FARPROC), &bytes, NULL, NULL))
    {
        goto ERROR_3;
    }

    // 绑定监听套接字
    if (SOCKET_ERROR == bind(pf->listen_sock,
        &pf->src_addr.addr, get_sockaddr_len(&pf->src_addr)))
    {
        goto ERROR_3;
    }

    // 将监听套接字与IOCP关联
    if (NULL == CreateIoCompletionPort((HANDLE)pf->listen_sock, pf->iocp, (ULONG_PTR)pf, 0))
    {
        goto ERROR_3;
    }

    // 监听开始
    if (SOCKET_ERROR == listen(pf->listen_sock, SOMAXCONN))
    {
        goto ERROR_3;
    }

    // 准备启动
    pf->running = 1;
    pf->post_count = 0;
    pf->active_accepts = 0;
    pf->active_threads = 0;

    // 创建线程
    for (int i = 0; i < pf->thread_count; i++)
    {
        // 创建工作线程
        InterlockedIncrement(&pf->active_threads);
        pf->thread_workers[i] = CreateThread(NULL, 0, thread_worker_callback, pf, 0, NULL);
        if (NULL == pf->thread_workers[i])
        {
            // 错误处理
            InterlockedExchange(&pf->running, 0);
            InterlockedDecrement(&pf->active_threads);

            for (int j = 0; j < i; j++)
            {
                PostQueuedCompletionStatus(pf->iocp, 0, 0, NULL);
            }

            for (int j = 0; j < i; j++)
            {
                WaitForSingleObject(pf->thread_workers[j], INFINITE);
                CloseHandle(pf->thread_workers[j]);
                pf->thread_workers[j] = NULL;
            }

            CloseHandle(pf->stop_event);
            pf->stop_event = NULL;
            goto ERROR_1;
        }
    }

    // 守护线程
    pf->thread_daemon = CreateThread(NULL, 0, thread_daemon_callback, pf, 0, NULL);
    if (NULL == pf->thread_daemon)
    {
        // 错误处理
        pf_tcp_stop(pf);
        goto ERROR_1;
    }

    return 0;

ERROR_3:
    closesocket(pf->listen_sock);
    pf->listen_sock = INVALID_SOCKET;
ERROR_2:
    CloseHandle(pf->stop_event);
    pf->stop_event = NULL;
ERROR_1:
    return -1;
}

// TCP转发停止
int pf_tcp_stop(pf_tcp_t* pf)
{
    if (NULL == pf || 0 == pf->running)
    {
        return -1;
    }

    InterlockedExchange(&pf->running, 0);

    if (NULL != pf->thread_daemon)
    {
        SetEvent(pf->stop_event);
        WaitForSingleObject(pf->thread_daemon, INFINITE);
        CloseHandle(pf->thread_daemon);
    }

    for (int i = 0; i < pf->thread_count; i++)
    {
        PostQueuedCompletionStatus(pf->iocp, 0, 0, NULL);
    }

    for (int i = 0; i < pf->thread_count; i++)
    {
        if (NULL != pf->thread_workers[i])
        {
            WaitForSingleObject(pf->thread_workers[i], INFINITE);
            CloseHandle(pf->thread_workers[i]);
            pf->thread_workers[i] = NULL;
        }
    }

    CloseHandle(pf->stop_event);
    pf->stop_event = NULL;
    return 0;
}

// 接受连接
static void handle_accept(pf_tcp_t* pf, pf_io_context_t* context, int is_success)
{
    // 继续投递
    if (0 == InterlockedCompareExchange(&pf->running, 0, 0) || -1 == post_accept(pf))
    {
        InterlockedDecrement(&pf->active_accepts);
    }

    // 处理事件
    int local_addr_len, remote_addr_len;
    pf_session_t* session = context->session;
    session_io_lock(session);

    // 出错
    if (0 == is_success)
    {
        goto ERROR_1;
    }

    // 更新信息
    if (SOCKET_ERROR == setsockopt(session->src_sock, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT,
        (char*)&pf->listen_sock, sizeof(SOCKET)))
    {
        goto ERROR_1;
    }

    // 获取地址
    pf->get_accept_sockaddrs(session->src_addr_buffer, 0, sizeof(pf_sockaddr_t) + 16, sizeof(pf_sockaddr_t) + 16,
        &session->src_local_addr, &local_addr_len, &session->src_remote_addr, &remote_addr_len);

    // 连接超时
    if (pf->config.timeout_ms > 0)
    {
        if (-1 == post_timeout(pf, session))
        {
            goto ERROR_1;
        }
    }

    // 连接目标
    if (-1 == post_connect(pf, session))
    {
        goto ERROR_2;
    }

    goto RESULT;

ERROR_2:
    if (pf->config.timeout_ms > 0)
    {
        session_cancel_timeout(session);
    }
ERROR_1:
    session_request_close(session, PF_SESSION_CLOSE);
RESULT:
    session_io_unlock(session);
    session_release(session);
}

// 连接处理
static void handle_connect(pf_tcp_t* pf, pf_io_context_t* context, int is_success)
{
    int addr_buffer_len;
    pf_session_t* session = context->session;
    session_io_lock(session);

    // 取消连接超时计时器
    if (pf->config.timeout_ms > 0)
    {
        session_cancel_timeout(session);
    }

    // 出错
    if (0 == is_success)
    {
        goto ERROR_1;
    }

    // 更新信息
    if (SOCKET_ERROR == setsockopt(session->dst_sock, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, NULL, 0))
    {
        goto ERROR_1;
    }

    // 获取地址
    addr_buffer_len = sizeof(pf_sockaddr_t) + 16;
    if (SOCKET_ERROR == getsockname(session->dst_sock, (struct sockaddr*)session->dst_addr_buffer, &addr_buffer_len))
    {
        goto ERROR_1;
    }

    addr_buffer_len = sizeof(pf_sockaddr_t) + 16;
    if (SOCKET_ERROR == getpeername(session->dst_sock, (struct sockaddr*)(session->dst_addr_buffer + sizeof(pf_sockaddr_t) + 16), &addr_buffer_len))
    {
        goto ERROR_1;
    }

    // 设置地址指针
    session->dst_local_addr = (SOCKADDR*)(session->dst_addr_buffer);
    session->dst_remote_addr = (SOCKADDR*)(session->dst_addr_buffer + sizeof(pf_sockaddr_t) + 16);

    // 投递源数据接收
    if (-1 == post_recv(pf, session, PF_IO_SRC_READ))
    {
        goto ERROR_1;
    }

    // 投递目标数据接收
    if (-1 == post_recv(pf, session, PF_IO_DST_READ))
    {
        goto ERROR_1;
    }

    goto RESULT;

ERROR_1:
    session_request_close(session, PF_SESSION_CLOSE);
RESULT:
    session_io_unlock(session);
    session_release(session);
}

// 源接收处理
static void handle_src_read(pf_tcp_t* pf, pf_io_context_t* context, int is_success, int bytes_transferred)
{
    // 会话指针
    pf_session_t* session = context->session;
    session_io_lock(session);

    // 接收出错
    if (0 == is_success)
    {
        goto ERROR_1;
    }

    // 对方关闭连接
    if (0 == bytes_transferred)
    {
        session_request_close(session, PF_SESSION_SRC_CLOSE);
        if (-1 == post_disconnect(pf, session, PF_IO_SRC_DISCONNECT))
        {
            goto ERROR_1;
        }

        goto RESULT;
    }

    // 投递目标发送
    if (-1 == post_send(pf, session, PF_IO_DST_WRITE, bytes_transferred, 0))
    {
        goto ERROR_1;
    }

    goto RESULT;

ERROR_1:
    session_request_close(session, PF_SESSION_CLOSE);
RESULT:
    session_io_unlock(session);
    session_release(session);
}

// 目标接收处理
static void handle_dst_read(pf_tcp_t* pf, pf_io_context_t* context, int is_success, int bytes_transferred)
{
    // 会话指针
    pf_session_t* session = context->session;
    session_io_lock(session);

    // 接收出错
    if (0 == is_success)
    {
        goto ERROR_1;
    }

    // 对方关闭连接
    if (0 == bytes_transferred)
    {
        session_request_close(session, PF_SESSION_DST_CLOSE);
        if (-1 == post_disconnect(pf, session, PF_IO_DST_DISCONNECT))
        {
            goto ERROR_1;
        }

        goto RESULT;
    }

    // 投递目标发送
    if (-1 == post_send(pf, session, PF_IO_SRC_WRITE, bytes_transferred, 0))
    {
        goto ERROR_1;
    }

    goto RESULT;

ERROR_1:
    session_request_close(session, PF_SESSION_CLOSE);
RESULT:
    session_io_unlock(session);
    session_release(session);
}

// 源发送处理
static void handle_src_write(pf_tcp_t* pf, pf_io_context_t* context, int is_success, int bytes_transferred)
{
    pf_session_t* session = context->session;
    session_io_lock(session);

    if (0 == is_success)
    {
        goto ERROR_1;
    }

    if (bytes_transferred < (int)session->dst_wsa_buffer.len)
    {
        // 发生部分发送，调整缓冲区并重新发送
        if (-1 == post_send(pf, session, PF_IO_SRC_WRITE, bytes_transferred, bytes_transferred))
        {
            goto ERROR_1;
        }
    }
    else
    {
        // 发送完成，可以接收来自目标的新数据了
        if (-1 == post_recv(pf, session, PF_IO_DST_READ))
        {
            goto ERROR_1;
        }
    }

    goto RESULT;

ERROR_1:
    session_request_close(session, PF_SESSION_CLOSE);
RESULT:
    session_io_unlock(session);
    session_release(session);
}

// 目标发送处理
static void handle_dst_write(pf_tcp_t* pf, pf_io_context_t* context, int is_success, int bytes_transferred)
{
    pf_session_t* session = context->session;
    session_io_lock(session);

    if (0 == is_success)
    {
        goto ERROR_1;
    }

    if (bytes_transferred < (int)session->src_wsa_buffer.len)
    {
        // 发生部分发送，调整缓冲区并重新发送
        if (-1 == post_send(pf, session, PF_IO_DST_WRITE, bytes_transferred, bytes_transferred))
        {
            goto ERROR_1;
        }
    }
    else
    {
        // 发送完成，可以接收来自源的新数据了
        if (-1 == post_recv(pf, session, PF_IO_SRC_READ))
        {
            goto ERROR_1;
        }
    }

    goto RESULT;

ERROR_1:
    session_request_close(session, PF_SESSION_CLOSE);
RESULT:
    session_io_unlock(session);
    session_release(session);
}

// 源套接字重用
static void handle_src_disconnect(pf_tcp_t* pf, pf_io_context_t* context, int is_success, int bytes_transferred)
{
    pf_session_t* session = context->session;
    session_io_lock(session);

    if (0 == is_success)
    {
        session_request_close(session, PF_SESSION_CLOSE);
    }

    session_io_unlock(session);
    session_release(session);
}

// 目标套接字重用
static void handle_dst_disconnect(pf_tcp_t* pf, pf_io_context_t* context, int is_success, int bytes_transferred)
{
    pf_session_t* session = context->session;
    session_io_lock(session);

    if (0 == is_success)
    {
        session_request_close(session, PF_SESSION_CLOSE);
    }

    session_io_unlock(session);
    session_release(session);
}

// 连接超时处理
static void handle_timeout(pf_tcp_t* pf, pf_io_context_t* context, int is_success, int bytes_transferred)
{
    pf_session_t* session = context->session;
    session_io_lock(session);

    if (0 != is_success && 0 != bytes_transferred)
    {
        session_request_close(session, PF_SESSION_CLOSE);
    }

    session_io_unlock(session);
    session_release(session);
}

// 定时器回调
static VOID CALLBACK handle_timer_callback(PVOID parameter, BOOLEAN timer_or_waitfired)
{
    pf_session_t* session = (pf_session_t*)parameter;
    (void)ChangeTimerQueueTimer(session->pf->timer_queue, session->timeout_timer, INFINITE, INFINITE);
    if (PF_SESSION_TIMEOUT_START == InterlockedCompareExchange(&session->timeout_status, PF_SESSION_TIMEOUT_COMPLETE, PF_SESSION_TIMEOUT_START))
    {
        PostQueuedCompletionStatus(session->pf->iocp, 1, (ULONG_PTR)session->pf, &session->tmo_io.overlapped);
        InterlockedExchange(&session->timeout_status, PF_SESSION_TIMEOUT_NONE);
    }
}

// 守护线程
static DWORD WINAPI thread_daemon_callback(LPVOID param)
{
    pf_tcp_t* pf = (pf_tcp_t*)param;
    for (;;)
    {
        // 退出信号
        if (0 == InterlockedCompareExchange(&pf->running, 0, 0))
        {
            break;
        }

        for (;;)
        {
            // 退出信号
            if (0 == InterlockedCompareExchange(&pf->running, 0, 0))
            {
                break;
            }

            // 投递数是否相等
            if (pf->total_accepts == InterlockedCompareExchange(
                &pf->active_accepts, pf->total_accepts, pf->total_accepts))
            {
                break;
            }

            // 补充投递
            InterlockedIncrement(&pf->active_accepts);
            if (-1 == post_accept(pf))
            {
                InterlockedDecrement(&pf->active_accepts);
                break;
            }
        }

        // 让出CPU
        WaitForSingleObject(pf->stop_event, 1000);
    }

    return 0;
}

// 工作线程
static DWORD WINAPI thread_worker_callback(LPVOID param)
{
    pf_tcp_t* pf = (pf_tcp_t*)param;
    for (;;)
    {
        // 获取完成状态
        DWORD bytes_transferred;
        ULONG_PTR completion_key;
        LPOVERLAPPED overlapped;
        BOOL is_success = GetQueuedCompletionStatus(
            pf->iocp,
            &bytes_transferred,
            &completion_key,
            &overlapped,
            INFINITE
        );

        // 流程控制
        if (0 == completion_key)
        {
            // 退出信号
            break;
        }
        else if (NULL != overlapped)
        {
            // 基础上下文
            InterlockedDecrement(&pf->post_count);
            pf_io_context_t* context = (pf_io_context_t*)overlapped;
            if (PF_IO_ACCEPT == context->operation_type)
            {
                // 接受连接
                handle_accept(pf, context, is_success);
            }
            else if (PF_IO_CONNECT == context->operation_type)
            {
                // 客户连接
                handle_connect(pf, context, is_success);
            }
            else if (PF_IO_SRC_READ == context->operation_type)
            {
                // 接收数据
                handle_src_read(pf, context, is_success, (int)bytes_transferred);
            }
            else if (PF_IO_SRC_WRITE == context->operation_type)
            {
                // 发送数据
                handle_src_write(pf, context, is_success, (int)bytes_transferred);
            }
            else if (PF_IO_DST_READ == context->operation_type)
            {
                // 接收数据
                handle_dst_read(pf, context, is_success, (int)bytes_transferred);
            }
            else if (PF_IO_DST_WRITE == context->operation_type)
            {
                // 发送数据
                handle_dst_write(pf, context, is_success, (int)bytes_transferred);
            }
            else if (PF_IO_SRC_DISCONNECT == context->operation_type)
            {
                // 重用套接字
                handle_src_disconnect(pf, context, is_success, (int)bytes_transferred);
            }
            else if (PF_IO_DST_DISCONNECT == context->operation_type)
            {
                // 重用套接字
                handle_dst_disconnect(pf, context, is_success, (int)bytes_transferred);
            }
            else if (PF_IO_TIMEOUT == context->operation_type)
            {
                // 超时处理
                handle_timeout(pf, context, is_success, (int)bytes_transferred);
            }
        }
    }

    // 最后一个线程负责清理工作
    if (0 == InterlockedDecrement(&pf->active_threads))
    {
        // 关闭监听套接字
        if (INVALID_SOCKET != pf->listen_sock)
        {
            closesocket(pf->listen_sock);
            pf->listen_sock = INVALID_SOCKET;
        }

        // 关闭所有会话
        pf_session_t* curr_session = NULL;
        ssize_t count = table_max_id(pf->session_table) + 1;
        for (ssize_t i = 0; i < count; i++)
        {
            if (-1 != table_get(pf->session_table, i, (uintptr_t*)&curr_session))
            {
                session_io_lock(curr_session);
                session_request_close(curr_session, PF_SESSION_CLOSE);
                session_io_unlock(curr_session);
            }
        }

        // 释放内存
        while (0 != InterlockedCompareExchange(&pf->post_count, 0, 0))
        {
            DWORD bytes_transferred;
            ULONG_PTR completion_key;
            LPOVERLAPPED overlapped;
            BOOL is_success = GetQueuedCompletionStatus(
                pf->iocp,
                &bytes_transferred,
                &completion_key,
                &overlapped,
                0
            );

            if (NULL != overlapped)
            {
                InterlockedDecrement(&pf->post_count);
                pf_io_context_t* context = (pf_io_context_t*)overlapped;
                session_release(context->session);
            }
            else
            {
                break;
            }
        }
    }

    return 0;
}

// 地址转换
static int parse_ip_addr(const char* ip, unsigned short port, pf_sockaddr_t* addr)
{
    // 尝试IPv4
    if (1 == inet_pton(AF_INET, ip, &addr->sin4.sin_addr))
    {
        addr->sin4.sin_family = AF_INET;
        addr->sin4.sin_port = htons(port);
        return 0;
    }

    // 尝试IPv6
    if (1 == inet_pton(AF_INET6, ip, &addr->sin6.sin6_addr))
    {
        addr->sin6.sin6_family = AF_INET6;
        addr->sin6.sin6_port = htons(port);
        return 0;
    }

    return -1;
}

// 获取地址长度
static int get_sockaddr_len(pf_sockaddr_t* sockaddr)
{
    if (AF_INET == sockaddr->addr.sa_family)
    {
        return sizeof(sockaddr->sin4);
    }
    else if (AF_INET6 == sockaddr->addr.sa_family)
    {
        return sizeof(sockaddr->sin6);
    }
    else
    {
        return 0;
    }
}

// 投递Accept
static int post_accept(pf_tcp_t* pf)
{
    DWORD bytes_received = 0;
    pf_session_t* session = session_create(pf);
    if (NULL == session)
    {
        goto ERROR_1;
    }

    session->src_io.operation_type = PF_IO_ACCEPT;
    memset(&session->src_io.overlapped, 0, sizeof(OVERLAPPED));

    InterlockedIncrement(&pf->post_count);
    if (FALSE == pf->acceptex(pf->listen_sock, session->src_sock,
        session->src_addr_buffer, 0, sizeof(pf_sockaddr_t) + 16, sizeof(pf_sockaddr_t) + 16,
        &bytes_received, &session->src_io.overlapped))
    {
        if (WSA_IO_PENDING != WSAGetLastError())
        {
            InterlockedDecrement(&pf->post_count);
            goto ERROR_2;
        }
    }

    return 0;

ERROR_2:
    session_release(session);
ERROR_1:
    return -1;
}

// 投递Connect
static int post_connect(pf_tcp_t* pf, pf_session_t* session)
{
    DWORD bytes_sent = 0;
    DWORD dst_addr_len = 0;
    session_ref(session);

    if (AF_INET == pf->dst_addr.addr.sa_family)
    {
        dst_addr_len = sizeof(pf->dst_addr.sin4);
    }
    else if (AF_INET6 == pf->dst_addr.addr.sa_family)
    {
        dst_addr_len = sizeof(pf->dst_addr.sin6);
    }
    else
    {
        goto ERROR_1;
    }

    session->dst_io.operation_type = PF_IO_CONNECT;
    memset(&session->dst_io.overlapped, 0, sizeof(OVERLAPPED));

    InterlockedIncrement(&pf->post_count);
    if (FALSE == pf->connectex(session->dst_sock, &pf->dst_addr.addr, dst_addr_len,
        NULL, 0, &bytes_sent, &session->dst_io.overlapped))
    {
        if (WSA_IO_PENDING != WSAGetLastError())
        {
            InterlockedDecrement(&pf->post_count);
            goto ERROR_1;
        }
    }

    return 0;

ERROR_1:
    session_release(session);
    return -1;
}

// 投递Timeout
static int post_timeout(pf_tcp_t* pf, pf_session_t* session)
{
    session_ref(session);
    if (PF_SESSION_TIMEOUT_NONE != InterlockedCompareExchange(&session->timeout_status, PF_SESSION_TIMEOUT_START, PF_SESSION_TIMEOUT_NONE))
    {
        session_release(session);
        return -1;
    }

    session->tmo_io.operation_type = PF_IO_TIMEOUT;
    memset(&session->tmo_io.overlapped, 0, sizeof(OVERLAPPED));

    InterlockedIncrement(&pf->post_count);
    if (FALSE == ChangeTimerQueueTimer(pf->timer_queue, session->timeout_timer, pf->config.timeout_ms, INFINITE))
    {
        InterlockedExchange(&session->timeout_status, PF_SESSION_TIMEOUT_NONE);
        InterlockedDecrement(&pf->post_count);
        session_release(session);
        return -1;
    }

    return 0;
}

// 投递Recv
static int post_recv(pf_tcp_t* pf, pf_session_t* session, pf_io_type_t type)
{
    DWORD flags = 0;
    WSABUF* buffer = NULL;
    pf_io_context_t* context = NULL;
    SOCKET sock = INVALID_SOCKET;
    session_ref(session);

    if (PF_IO_SRC_READ == type)
    {
        sock = session->src_sock;
        context = &session->src_io;
        buffer = &session->src_wsa_buffer;
        buffer->buf = session->src_data_buffer;
        buffer->len = sizeof(session->src_data_buffer);
    }
    else if (PF_IO_DST_READ == type)
    {
        sock = session->dst_sock;
        context = &session->dst_io;
        buffer = &session->dst_wsa_buffer;
        buffer->buf = session->dst_data_buffer;
        buffer->len = sizeof(session->dst_data_buffer);
    }
    else
    {
        goto ERROR_1;
    }

    context->operation_type = type;
    memset(&context->overlapped, 0, sizeof(OVERLAPPED));

    InterlockedIncrement(&pf->post_count);
    if (SOCKET_ERROR == WSARecv(sock, buffer, 1, NULL, &flags, &context->overlapped, NULL))
    {
        if (WSA_IO_PENDING != WSAGetLastError())
        {
            InterlockedDecrement(&pf->post_count);
            goto ERROR_1;
        }
    }

    return 0;

ERROR_1:
    session_release(session);
    return -1;
}

// 投递Send
static int post_send(pf_tcp_t* pf, pf_session_t* session, pf_io_type_t type, int bytes_transferred, int offset)
{
    WSABUF* buffer = NULL;
    pf_io_context_t* context = NULL;
    SOCKET sock = INVALID_SOCKET;
    session_ref(session);

    if (PF_IO_SRC_WRITE == type)
    {
        sock = session->src_sock;
        context = &session->dst_io;
        buffer = &session->dst_wsa_buffer;
        if (offset > 0)
        {
            buffer->buf += offset;
            buffer->len -= offset;
        }
        else
        {
            buffer->buf = session->dst_data_buffer;
            buffer->len = bytes_transferred;
        }
    }
    else if (PF_IO_DST_WRITE == type)
    {
        sock = session->dst_sock;
        context = &session->src_io;
        buffer = &session->src_wsa_buffer;
        if (offset > 0)
        {
            buffer->buf += offset;
            buffer->len -= offset;
        }
        else
        {
            buffer->buf = session->src_data_buffer;
            buffer->len = bytes_transferred;
        }
    }
    else
    {
        goto ERROR_1;
    }

    context->operation_type = type;
    memset(&context->overlapped, 0, sizeof(OVERLAPPED));

    InterlockedIncrement(&pf->post_count);
    if (SOCKET_ERROR == WSASend(sock, buffer, 1, NULL, 0, &context->overlapped, NULL))
    {
        if (WSA_IO_PENDING != WSAGetLastError())
        {
            InterlockedDecrement(&pf->post_count);
            goto ERROR_1;
        }
    }

    return 0;

ERROR_1:
    session_release(session);
    return -1;
}

// 投递Disconnect
static int post_disconnect(pf_tcp_t* pf, pf_session_t* session, pf_io_type_t type)
{
    SOCKET sock = INVALID_SOCKET;
    pf_io_context_t* context = NULL;
    session_ref(session);

    if (PF_IO_SRC_DISCONNECT == type)
    {
        sock = session->src_sock;
        context = &session->src_io;
    }
    else if (PF_IO_DST_DISCONNECT == type)
    {
        sock = session->dst_sock;
        context = &session->dst_io;
    }
    else
    {
        goto ERROR_1;
    }

    context->operation_type = type;
    memset(&context->overlapped, 0, sizeof(OVERLAPPED));

    InterlockedIncrement(&pf->post_count);
    if (SOCKET_ERROR == pf->disconnectex(sock, &context->overlapped, TF_REUSE_SOCKET, 0))
    {
        if (WSA_IO_PENDING != WSAGetLastError())
        {
            InterlockedDecrement(&pf->post_count);
            goto ERROR_1;
        }
    }

    return 0;

ERROR_1:
    session_release(session);
    return -1;
}

// 会话池空时触发申请操作
static void* session_pool_alloc_t(pool_t* pool, void* user_data)
{
    pf_tcp_t* pf = (pf_tcp_t*)user_data;
    pf_session_t* session = (pf_session_t*)pool_mem_alloc(pool, sizeof(pf_session_t));
    if (NULL == session)
    {
        goto ERROR_1;
    }

    // 初始化
    session->ref = 1;
    session->lock = 0;
    session->session_id = -1;
    session->close_flags = PF_SESSION_NONE;
    session->timeout_status = PF_SESSION_TIMEOUT_NONE;
    session->pf = pf;

    session->src_local_addr = NULL;
    session->src_remote_addr = NULL;
    session->src_wsa_buffer.buf = NULL;
    session->src_wsa_buffer.len = 0;

    session->dst_local_addr = NULL;
    session->dst_remote_addr = NULL;
    session->dst_wsa_buffer.buf = NULL;
    session->dst_wsa_buffer.len = 0;

    memset(&session->src_io, 0, sizeof(pf_io_context_t));
    memset(&session->dst_io, 0, sizeof(pf_io_context_t));
    memset(&session->tmo_io, 0, sizeof(pf_io_context_t));
    session->src_io.session = session;
    session->dst_io.session = session;
    session->tmo_io.session = session;

    // 创建源套接字
    session->src_sock = WSASocketW(pf->src_addr.addr.sa_family,
        SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
    if (INVALID_SOCKET == session->src_sock)
    {
        goto ERROR_2;
    }

    // 将源套接字与IOCP关联
    if (NULL == CreateIoCompletionPort((HANDLE)session->src_sock, pf->iocp, (ULONG_PTR)pf, 0))
    {
        goto ERROR_3;
    }

    // 创建目标套接字
    session->dst_sock = WSASocketW(pf->dst_addr.addr.sa_family,
        SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
    if (INVALID_SOCKET == session->dst_sock)
    {
        goto ERROR_3;
    }

    // 目标套接字绑定出站地址
    if (SOCKET_ERROR == bind(session->dst_sock,
        &pf->out_addr.addr, get_sockaddr_len(&pf->out_addr)))
    {
        goto ERROR_4;
    }

    // 将连接套接字与IOCP关联
    if (NULL == CreateIoCompletionPort((HANDLE)session->dst_sock, pf->iocp, (ULONG_PTR)pf, 0))
    {
        goto ERROR_4;
    }

    // 创建超时定时器
    if (FALSE == CreateTimerQueueTimer(&session->timeout_timer, pf->timer_queue,
        handle_timer_callback, session, INFINITE, INFINITE, 0))
    {
        goto ERROR_4;
    }

    return session;

ERROR_4:
    closesocket(session->dst_sock);
ERROR_3:
    closesocket(session->src_sock);
ERROR_2:
    pool_mem_free(pool, session);
ERROR_1:
    return NULL;
}

// 会话池回收时触发
static void* session_pool_reset(pool_t* pool, void* user_data, void* mem)
{
    pf_tcp_t* pf = (pf_tcp_t*)user_data;
    pf_session_t* session = (pf_session_t*)mem;

    // 重新初始化
    session->ref = 1;
    session->lock = 0;
    session->session_id = -1;
    session->close_flags = PF_SESSION_NONE;
    session->timeout_status = PF_SESSION_TIMEOUT_NONE;

    session->src_local_addr = NULL;
    session->src_remote_addr = NULL;
    session->src_wsa_buffer.buf = NULL;
    session->src_wsa_buffer.len = 0;

    session->dst_local_addr = NULL;
    session->dst_remote_addr = NULL;
    session->dst_wsa_buffer.buf = NULL;
    session->dst_wsa_buffer.len = 0;

    memset(&session->src_io, 0, sizeof(pf_io_context_t));
    memset(&session->dst_io, 0, sizeof(pf_io_context_t));
    memset(&session->tmo_io, 0, sizeof(pf_io_context_t));
    session->src_io.session = session;
    session->dst_io.session = session;
    session->tmo_io.session = session;

    // 是否需要创建源套接字
    if (INVALID_SOCKET == session->src_sock)
    {
        // 创建源套接字
        session->src_sock = WSASocketW(pf->src_addr.addr.sa_family,
            SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
        if (INVALID_SOCKET == session->src_sock)
        {
            return NULL;
        }

        // 将源套接字与IOCP关联
        if (NULL == CreateIoCompletionPort((HANDLE)session->src_sock, pf->iocp, (ULONG_PTR)pf, 0))
        {
            closesocket(session->src_sock);
            session->src_sock = INVALID_SOCKET;
            return NULL;
        }
    }

    // 是否需要创建目标套接字
    if (INVALID_SOCKET == session->dst_sock)
    {
        // 创建目标套接字
        session->dst_sock = WSASocketW(pf->dst_addr.addr.sa_family,
            SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
        if (INVALID_SOCKET == session->dst_sock)
        {
            return NULL;
        }

        if (SOCKET_ERROR == bind(session->dst_sock,
            &pf->out_addr.addr, get_sockaddr_len(&pf->out_addr)))
        {
            closesocket(session->dst_sock);
            session->dst_sock = INVALID_SOCKET;
            return NULL;
        }

        // 将连接套接字与IOCP关联
        if (NULL == CreateIoCompletionPort((HANDLE)session->dst_sock, pf->iocp, (ULONG_PTR)pf, 0))
        {
            closesocket(session->dst_sock);
            session->dst_sock = INVALID_SOCKET;
            return NULL;
        }
    }

    return mem;
}

// 会话池关闭时触发
static void session_pool_free(pool_t* pool, void* user_data, void* mem)
{
    pf_tcp_t* pf = (pf_tcp_t*)user_data;
    pf_session_t* session = (pf_session_t*)mem;

    if (NULL != session->timeout_timer)
    {
        (void)DeleteTimerQueueTimer(pf->timer_queue,
            session->timeout_timer, INVALID_HANDLE_VALUE);
        session->timeout_timer = NULL;
    }

    if (INVALID_SOCKET != session->src_sock)
    {
        closesocket(session->src_sock);
        session->src_sock = INVALID_SOCKET;
    }

    if (INVALID_SOCKET != session->dst_sock)
    {
        closesocket(session->dst_sock);
        session->dst_sock = INVALID_SOCKET;
    }

    pool_mem_free(pool, mem);
}

// 创建会话池
static pool_t* session_pool_create(pf_tcp_t* pf, size_t max_size)
{
    return pool_create(pf, max_size, 0, session_pool_alloc_t, session_pool_reset, session_pool_free);
}

// 关闭会话池
static void session_pool_close(pool_t* pool)
{
    pool_close(pool);
}

// 创建会话
static pf_session_t* session_create(pf_tcp_t* pf)
{
    // 池子里取出会话指针
    pf_session_t* session = (pf_session_t*)pool_get(pf->session_pool);

    // 会话表分配唯一ID
    session->session_id = table_acquire_id(pf->session_table);
    if (-1 == session->session_id)
    {
        goto ERROR_1;
    }

    // 写入会话指针
    if (-1 == table_set(pf->session_table, session->session_id, (uintptr_t)session))
    {
        goto ERROR_2;
    }

    return session;

ERROR_2:
    table_free_id(pf->session_table, session->session_id);
ERROR_1:
    pool_put(pf->session_pool, session);
    return NULL;
}

// 增加会话引用
static void session_ref(pf_session_t* session)
{
    InterlockedIncrement(&session->ref);
}

// 会话加锁
static void session_io_lock(pf_session_t* session)
{
    UINT spin_count = 0;
    for (;;)
    {
        LONG current = InterlockedCompareExchange(&session->lock, 0, 0);
        if (-1 == current)
        {
            // 独占锁
            if (spin_count++ < 8000)
            {
                // 短暂让出CPU
                YieldProcessor();
            }
            else
            {
                // 让出CPU
                spin_count = 0;
                Sleep(0);
            }
            
            continue;
        }

        // 尝试原子增加计数(current = current + 1)
        if (current == InterlockedCompareExchange(&session->lock, current + 1, current))
        {
            // 加锁成功
            break;
        }
    }
}

// 会话解锁
static void session_io_unlock(pf_session_t* session)
{
    for (;;)
    {
        // 尝试原子减少计数(current = current - 1)，= 0 时进入独占锁
        LONG current = InterlockedCompareExchange(&session->lock, 0, 0);
        if (current == InterlockedCompareExchange(&session->lock, current > 1 ? current - 1 : -1, current))
        {
            // 独占锁
            // 安全关闭套接字
            LONG flags = InterlockedCompareExchange(&session->close_flags, 0, 0);
            if (PF_SESSION_CLOSE == (flags & PF_SESSION_CLOSE))
            {
                // 强制关闭套接字
                if (PF_SESSION_CLOSE_END != (flags & PF_SESSION_CLOSE_END))
                {
                    InterlockedOr(&session->close_flags, PF_SESSION_CLOSE_END);
                    if (INVALID_SOCKET != session->src_sock)
                    {
                        closesocket(session->src_sock);
                        session->src_sock = INVALID_SOCKET;
                    }
                    if (INVALID_SOCKET != session->dst_sock)
                    {
                        closesocket(session->dst_sock);
                        session->dst_sock = INVALID_SOCKET;
                    }
                }
            }

            // 源端读到了 FIN (bytes==0)，需要向目标端发送 FIN
            if (PF_SESSION_SRC_CLOSE == (flags & PF_SESSION_SRC_CLOSE))
            {
                if (PF_SESSION_SRC_CLOSE_END != (flags & PF_SESSION_SRC_CLOSE_END))
                {
                    InterlockedOr(&session->close_flags, PF_SESSION_SRC_CLOSE_END);
                    if (INVALID_SOCKET != session->dst_sock)
                    {
                        shutdown(session->dst_sock, SD_SEND);
                    }
                }
            }

            // 目标端读到了 FIN (bytes==0)，需要向源端发送 FIN
            if (PF_SESSION_DST_CLOSE == (flags & PF_SESSION_DST_CLOSE))
            {
                if (PF_SESSION_DST_CLOSE_END != (flags & PF_SESSION_DST_CLOSE_END))
                {
                    InterlockedOr(&session->close_flags, PF_SESSION_DST_CLOSE_END);
                    if (INVALID_SOCKET != session->src_sock)
                    {
                        shutdown(session->src_sock, SD_SEND);
                    }
                }
            }

            // 解锁
            InterlockedExchange(&session->lock, 0);
            break;
        }
    }
}

// 会话请求关闭
static void session_request_close(pf_session_t* session, pf_session_close_flag_t flag)
{
    InterlockedOr(&session->close_flags, flag);
}

// 会话取消超时
static void session_cancel_timeout(pf_session_t* session)
{
    pf_tcp_t* pf = session->pf;
    if (PF_SESSION_TIMEOUT_START == InterlockedCompareExchange(&session->timeout_status, PF_SESSION_TIMEOUT_END, PF_SESSION_TIMEOUT_START))
    {
        PostQueuedCompletionStatus(pf->iocp, 0, (ULONG_PTR)pf, &session->tmo_io.overlapped);
        (void)ChangeTimerQueueTimer(pf->timer_queue, session->timeout_timer, INFINITE, INFINITE);
        InterlockedExchange(&session->timeout_status, PF_SESSION_TIMEOUT_NONE);
    }
}

// 释放会话
static void session_release(pf_session_t* session)
{
    if (0 == InterlockedDecrement(&session->ref))
    {
        pf_tcp_t* pf = session->pf;
        table_free_id(pf->session_table, session->session_id);
        (void)ChangeTimerQueueTimer(pf->timer_queue, session->timeout_timer, INFINITE, INFINITE);
        pool_put(pf->session_pool, session);
    }
}
