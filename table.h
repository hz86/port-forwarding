#ifndef TABLE_H
#define TABLE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

	typedef intptr_t ssize_t;
	typedef struct table_t table_t;

    // 创建表 block_size 必须是 2的幂
    table_t* table_create(size_t capacity, size_t block_size);

    // 表获取最大ID
    ssize_t table_max_id(table_t* table);

    // 表获取唯一ID
    ssize_t table_acquire_id(table_t* table);

    // 表删除ID
    int table_free_id(table_t* table, ssize_t id);

    // 表设置数据
    int table_set(table_t* table, ssize_t id, uintptr_t data);

    // 表获取数据
    int table_get(table_t* table, ssize_t id, uintptr_t* data);

    // 关闭表
    void table_close(table_t* table);

#ifdef __cplusplus
}
#endif

#endif // TABLE_H
