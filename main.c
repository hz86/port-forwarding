#define _CRT_SECURE_NO_WARNINGS

#include <Windows.h>
#include <stdio.h>
#include <stdlib.h>
#include "fwding.h"

volatile int is_exit = 0;

BOOL WINAPI console_handler(DWORD ctrltype)
{
    is_exit = 1;
    Sleep(1000 * 3);
    return TRUE;
}

int main()
{
    SetConsoleCtrlHandler(console_handler, TRUE);

    pf_config_t config;
    memset(&config, 0, sizeof(pf_config_t));

    strcpy(config.src_ip, "127.0.0.1");
    strcpy(config.dst_ip, "127.0.0.1");
    strcpy(config.out_ip, "0.0.0.0");
    config.src_port = 8080;
    config.dst_port = 80;
    config.timeout_ms = 1000;

    pf_tcp_t* pf = pf_tcp_create(&config);

    pf_tcp_start(pf);

    while(0 == is_exit)
    {
        Sleep(1000);
    }

    pf_tcp_stop(pf);

    pf_tcp_destroy(pf);
}
