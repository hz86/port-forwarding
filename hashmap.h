#ifndef HASHMAP_H
#define HASHMAP_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

    // 句柄
    typedef struct hashmap_t hashmap_t;
    typedef struct hashmap_iterator_t hashmap_iterator_t;

    // 哈希节点
    typedef struct hashmap_node_t hashmap_node_t;
    typedef struct hashmap_node_t {
        hashmap_node_t* next;    // 下一个节点
        size_t keylen;           // 键长度
        void* keyname;           // 键指针
        void* value;             // 关联值
        size_t hash;             // 哈希值
    } hashmap_node_t;

    // 哈希表迭代器
    typedef struct hashmap_iterator_t {
        hashmap_iterator_t* next;   // 下一个迭代器
        hashmap_t* map;             // 所属的表
        hashmap_node_t* next_node;  // 预取的下一个节点
        size_t next_index;          // 预取节点的索引
        int is_old_table;           // 是否旧表
    } hashmap_iterator_t;

    // key的比较回调 相等返回0
    typedef int (*hashmap_compare_cb)(hashmap_node_t* a, hashmap_node_t* b);

    // 创建哈希表
    // capacity 必须2的幂, factor 0-100
    hashmap_t* hashmap_create(size_t capacity, size_t factor, hashmap_compare_cb compare);

    // 哈希表查找元素
    // 返回查找到的节点指针，失败返回NULL
    hashmap_node_t* hashmap_find(hashmap_t* map, hashmap_node_t* node);

    // 哈希表插入元素
    // 插入成功，返回新节点，如果冲突，返回旧节点，失败返回NULL
    hashmap_node_t* hashmap_insert(hashmap_t* map, hashmap_node_t* node);

    // 哈希表移除元素
    // 返回移除的节点，移除失败返回NULL
    hashmap_node_t* hashmap_remove(hashmap_t* map, hashmap_node_t* node);

    // 迭代器开始
    hashmap_node_t* hashmap_iterate_begin(hashmap_t* map, hashmap_iterator_t* iter);

    // 获取下一个节点
    // 使用 hashmap_iterate_begin 初始化过的 iter
    hashmap_node_t* hashmap_iterate_next(hashmap_iterator_t* iter);

    // 迭代器结束
    void hashmap_iterate_end(hashmap_iterator_t* iter);

    // 哈希表大小
    size_t hashmap_size(hashmap_t* map);

    // 关闭哈希表
    void hashmap_close(hashmap_t* map);

#ifdef __cplusplus
}
#endif

#endif // HASHMAP_H
