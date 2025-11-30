#ifndef FWDING_H
#define FWDING_H

#ifdef __cplusplus
extern "C" {
#endif

    // 操作句柄
    typedef struct pf_tcp_t pf_tcp_t;

    // 配置信息
    typedef struct pf_config_t {
        char src_ip[46];         // 源始IP 
        char dst_ip[46];         // 转发IP
        char out_ip[46];         // 出站IP，可空
        unsigned short src_port; // 源始端口
        unsigned short dst_port; // 转发端口
        unsigned int timeout_ms; // 超时时间，毫秒
    } pf_config_t;

    // TCP转发创建
    pf_tcp_t* pf_tcp_create(pf_config_t* config);

    // TCP转发启动
    int pf_tcp_start(pf_tcp_t* pf);

    // TCP转发停止
    int pf_tcp_stop(pf_tcp_t* pf);

    // TCP转发销毁
    void pf_tcp_destroy(pf_tcp_t* pf);

#ifdef __cplusplus
}
#endif

#endif // FWDING_H
