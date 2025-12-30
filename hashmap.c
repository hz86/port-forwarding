#define _CRT_SECURE_NO_WARNINGS

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <windows.h>
#include "hashmap.h"

// 哈希表状态
typedef enum {
    HASHMAP_NONE = 0,
    HASHMAP_REHASH = 1,
    HASHMAP_RESIZE = 2,
} hashmap_status_t;

// 哈希表结构
typedef struct hashmap_t {
    SRWLOCK lock;                  // 读写锁
    hashmap_iterator_t* iter_head; // 迭代器头
    hashmap_compare_cb compare;    // 比较回调
    hashmap_node_t** table;        // 节点数组
    hashmap_node_t** old_table;    // 旧节点数组
    size_t old_capacity;           // 旧容量大小
    size_t move_index;             // 迁移索引
    size_t capacity;               // 容量大小
    size_t factor;                 // 负载因子
    size_t size;                   // 表大小
    size_t status;                 // 表状态
} hashmap_t;

// 计算是否2的幂
static inline int _is_power_of_two(size_t n) {
    return (n > 0) && ((n & (n - 1)) == 0);
}

// 计算哈希32位
static inline uint32_t _murmurhash2_32(const void* key, uint32_t len, uint32_t seed)
{
    // 'm' and 'r' are mixing constants generated offline.
    // They're not really 'magic', they just happen to work well.

    const uint32_t m = 0x5bd1e995;
    const int r = 24;

    // Initialize the hash to a 'random' value

    uint32_t h = seed ^ len;

    // Mix 4 bytes at a time into the hash

    const unsigned char* data = (const unsigned char*)key;

    while (len >= 4)
    {
        uint32_t k;
        memcpy(&k, data, sizeof(uint32_t));

        k *= m;
        k ^= k >> r;
        k *= m;

        h *= m;
        h ^= k;

        data += 4;
        len -= 4;
    }

    // Handle the last few bytes of the input array

    switch (len)
    {
    case 3: h ^= data[2] << 16;
    case 2: h ^= data[1] << 8;
    case 1: h ^= data[0];
        h *= m;
    };

    // Do a few final mixes of the hash to ensure the last few
    // bytes are well-incorporated.

    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;

    return h;
}

// 计算哈希64位
static inline uint64_t _murmurhash2_64(const void* key, uint64_t len, uint64_t seed)
{
    const uint64_t m = 0xc6a4a7935bd1e995LLU;
    const int r = 47;

    uint64_t h = seed ^ (len * m);
    const unsigned char* data = (const unsigned char*)key;

    while (len >= 8)
    {
        uint64_t k;
        memcpy(&k, data, sizeof(uint64_t));

        k *= m;
        k ^= k >> r;
        k *= m;

        h *= m;
        h ^= k;

        data += 8;
        len -= 8;
    }

    switch (len)
    {
    case 7: h ^= (uint64_t)(data[6]) << 48;
    case 6: h ^= (uint64_t)(data[5]) << 40;
    case 5: h ^= (uint64_t)(data[4]) << 32;
    case 4: h ^= (uint64_t)(data[3]) << 24;
    case 3: h ^= (uint64_t)(data[2]) << 16;
    case 2: h ^= (uint64_t)(data[1]) << 8;
    case 1: h ^= (uint64_t)(data[0]);
        h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

// 计算哈希
static inline size_t hashmap_hash(const void* key, size_t len, size_t seed)
{
    if (sizeof(uint64_t) == sizeof(size_t))
    {
        return (size_t)_murmurhash2_64(key, (uint64_t)len, (uint64_t)seed);
    }
    else if (sizeof(uint32_t) == sizeof(size_t))
    {
        return (size_t)_murmurhash2_32(key, (uint32_t)len, (uint32_t)seed);
    }
    else
    {
        return 0;
    }
}

//计算索引
static inline size_t hashmap_index_for(size_t h, size_t len)
{
    return h & (len - 1);
}

// 扫描链表
static inline hashmap_node_t* hashmap_list_scan(
    hashmap_node_t* head, hashmap_node_t* target, size_t target_hash,
    hashmap_compare_cb compare, hashmap_node_t** out_prev)
{
    hashmap_node_t* cur = head;
    hashmap_node_t* prev = NULL;

    while (NULL != cur)
    {
        if (target_hash == cur->hash && 0 == compare(cur, target))
        {
            if (NULL != out_prev) *out_prev = prev;
            break;
        }

        prev = cur;
        cur = cur->next;
    }

    return cur;
}

// 迭代器查找下一个节点
static inline hashmap_node_t* hashmap_iterate_find_next(hashmap_iterator_t* iter)
{
    hashmap_t* map = iter->map;
    hashmap_node_t* cur = iter->next_node;

    // 直接获取下一个
    if (NULL != cur) cur = cur->next;
    if (NULL != cur) return cur;
    
    if (0 != iter->is_old_table)
    {
        // 查找旧表节点
        while (iter->next_index < map->old_capacity)
        {
            if (NULL != map->old_table[iter->next_index])
            {
                // 已找到
                cur = map->old_table[iter->next_index++];
                break;
            }

            iter->next_index++;
        }

        // 是否查找完毕
        if (iter->next_index >= map->old_capacity)
        {
            // 切换到新表
            iter->next_index = 0;
            iter->is_old_table = 0;
        }
    }

    if (NULL == cur)
    {
        // 查询新表
        while (iter->next_index < map->capacity)
        {
            if (NULL != map->table[iter->next_index])
            {
                // 已找到
                cur = map->table[iter->next_index++];
                break;
            }

            iter->next_index++;
        }
    }
    
    return cur;
}

// 更新受影响的迭代器
static inline void hashmap_update_iterators(hashmap_t* map, hashmap_node_t* removed_node)
{
    hashmap_iterator_t* iter = map->iter_head;
    while (NULL != iter)
    {
        if (iter->next_node == removed_node)
        {
            // 迭代器指向的节点即将被移除，向前推进一步
            iter->next_node = hashmap_iterate_find_next(iter);
        }

        iter = iter->next;
    }
}

// 迁移哈希表
static hashmap_node_t** hashmap_rehash(hashmap_t* map)
{
    if (HASHMAP_REHASH != map->status)
    {
        return NULL;
    }

    // 一次迁移1%
    size_t count = map->old_capacity / 100;
    if (count > 1000) count = 1000;
    if (count < 2) count = 2;

    hashmap_node_t** free_table = NULL;
    for (size_t i = 0; i < count; i++)
    {
        // 旧表数据
        hashmap_node_t* cur = map->old_table[map->move_index];
        map->old_table[map->move_index] = NULL;
        while (NULL != cur)
        {
            hashmap_node_t* next = cur->next;
            size_t idx = hashmap_index_for(cur->hash, map->capacity);

            // 头插法迁移到新表
            cur->next = map->table[idx];
            map->table[idx] = cur;

            cur = next;
        }

        // 下一个
        map->move_index++;
        if (map->move_index >= map->old_capacity)
        {
            map->status = HASHMAP_NONE;
            free_table = map->old_table;
            map->old_table = NULL;
            map->old_capacity = 0;
            map->move_index = 0;
            break;
        }
    }
    
    return free_table;
}

// 判断是否需要扩容
static size_t hashmap_resize_decide(hashmap_t* map)
{
    if (HASHMAP_NONE != map->status)
    {
        return 0;
    }

    // 是否需要扩容
    size_t threshold = (size_t)((uint64_t)map->capacity * map->factor / 100);
    if (map->size < threshold)
    {
        return 0;
    }

    // 容量翻倍
    size_t old_capacity = map->capacity;
    size_t new_capacity = old_capacity << 1;
    if (new_capacity < old_capacity)
    {
        return 0;
    }

    // 需要扩容
    map->status = HASHMAP_RESIZE;
    return new_capacity;
}

// 扩容哈希表
static void hashmap_resize(hashmap_t* map, size_t new_capacity)
{
    int is_success = 0;
    hashmap_node_t** new_table = (hashmap_node_t**)calloc(new_capacity, sizeof(hashmap_node_t*));
    AcquireSRWLockExclusive(&map->lock);

    if (HASHMAP_RESIZE == map->status)
    {
        // 必须无迭代器，有则放弃扩容
        if (NULL != new_table && NULL == map->iter_head)
        {
            // 扩容成功
            map->move_index = 0;
            map->old_capacity = map->capacity;
            map->old_table = map->table;
            map->table = new_table;
            map->capacity = new_capacity;
            map->status = HASHMAP_REHASH;
            is_success = 1;
        }
        else
        {
            // 放弃扩容
            map->status = HASHMAP_NONE;
        }
    }

    ReleaseSRWLockExclusive(&map->lock);
    if (0 == is_success && NULL != new_table)
    {
        free(new_table);
    }
}

// 创建哈希表
// capacity 必须2的幂, factor 0-100
hashmap_t* hashmap_create(size_t capacity, size_t factor, hashmap_compare_cb compare)
{
    if (NULL == compare || 0 == _is_power_of_two(capacity) || factor > 100)
    {
        return NULL;
    }

    hashmap_t* map = (hashmap_t*)malloc(sizeof(hashmap_t));
    if (NULL == map)
    {
        return NULL;
    }

    map->table = (hashmap_node_t**)calloc(capacity, sizeof(hashmap_node_t*));
    if (NULL == map->table)
    {
        free(map);
        return NULL;
    }

    map->size = 0;
    map->move_index = 0;
    map->old_capacity = 0;
    map->old_table = NULL;
    map->iter_head = NULL;
    map->status = HASHMAP_NONE;

    map->factor = factor;
    map->capacity = capacity;
    map->compare = compare;
    
    InitializeSRWLock(&map->lock);
    return map;
}

// 哈希表查找元素
// 返回查找到的节点指针，失败返回NULL
hashmap_node_t* hashmap_find(hashmap_t* map, hashmap_node_t* node)
{
    if (NULL == map || NULL == node) return NULL;
    size_t h = hashmap_hash(node->keyname, node->keylen, 0);
    AcquireSRWLockShared(&map->lock);

    // 默认表
    size_t idx = hashmap_index_for(h, map->capacity);
    hashmap_node_t* cur = hashmap_list_scan(map->table[idx], node, h, map->compare, NULL);
    if (NULL == cur && HASHMAP_REHASH == map->status)
    {
        // 迁移中，查询旧表
        size_t old_idx = hashmap_index_for(h, map->old_capacity);
        cur = hashmap_list_scan(map->old_table[old_idx], node, h, map->compare, NULL);
    }

    ReleaseSRWLockShared(&map->lock);
    return cur;
}

// 哈希表插入元素
// 插入成功，返回新节点，如果冲突，返回旧节点，失败返回NULL
hashmap_node_t* hashmap_insert(hashmap_t* map, hashmap_node_t* node)
{
    if (NULL == map || NULL == node) return NULL;
    size_t h = hashmap_hash(node->keyname, node->keylen, 0);
    AcquireSRWLockExclusive(&map->lock);

    // 默认表
    size_t idx = hashmap_index_for(h, map->capacity);
    hashmap_node_t* cur = hashmap_list_scan(map->table[idx], node, h, map->compare, NULL);
    if (NULL == cur && HASHMAP_REHASH == map->status)
    {
        // 迁移中，查询旧表
        size_t old_idx = hashmap_index_for(h, map->old_capacity);
        cur = hashmap_list_scan(map->old_table[old_idx], node, h, map->compare, NULL);
    }

    if (NULL == cur)
    {
        // 头插法插入
        node->hash = h;
        node->next = map->table[idx];
        map->table[idx] = node;
        map->size++;
        cur = node;
    }

    size_t new_capacity = 0;
    hashmap_node_t** free_table = NULL;
    if (NULL == map->iter_head)
    {
        // 无迭代器时才能扩容和迁移
        free_table = hashmap_rehash(map);
        new_capacity = hashmap_resize_decide(map);
    }

    ReleaseSRWLockExclusive(&map->lock);
    if (0 != new_capacity)
    {
        hashmap_resize(map, new_capacity);
    }

    if (NULL != free_table)
    {
        free(free_table);
    }

    return cur;
}

// 哈希表移除元素
// 返回移除的节点，移除失败返回NULL
hashmap_node_t* hashmap_remove(hashmap_t* map, hashmap_node_t* node)
{
    if (NULL == map || NULL == node) return NULL;
    size_t h = hashmap_hash(node->keyname, node->keylen, 0);
    AcquireSRWLockExclusive(&map->lock);

    hashmap_node_t* prev = NULL;
    size_t idx = hashmap_index_for(h, map->capacity);
    hashmap_node_t* cur = hashmap_list_scan(map->table[idx], node, h, map->compare, &prev);
    if (NULL != cur)
    {
        // 找到节点，执行断链
        if (NULL != prev) prev->next = cur->next;
        else map->table[idx] = cur->next;
        map->size--;
    }
    else if (HASHMAP_REHASH == map->status)
    {
        // 在旧表中查找并尝试移除
        size_t old_idx = hashmap_index_for(h, map->old_capacity);
        cur = hashmap_list_scan(map->old_table[old_idx], node, h, map->compare, &prev);
        if (NULL != cur)
        {
            // 找到节点，执行断链
            if (prev) prev->next = cur->next;
            else map->old_table[old_idx] = cur->next;
            map->size--;
        }
    }

    if (NULL != cur)
    {
        // 更新受影响的迭代器
        hashmap_update_iterators(map, cur);
        cur->next = NULL;
    }

    size_t new_capacity = 0;
    hashmap_node_t** free_table = NULL;
    if (NULL == map->iter_head)
    {
        // 无迭代器时才能扩容和迁移
        free_table = hashmap_rehash(map);
        new_capacity = hashmap_resize_decide(map);
    }

    ReleaseSRWLockExclusive(&map->lock);
    if (0 != new_capacity)
    {
        hashmap_resize(map, new_capacity);
    }

    if (NULL != free_table)
    {
        free(free_table);
    }

    return cur;
}

// 迭代器开始
hashmap_node_t* hashmap_iterate_begin(hashmap_t* map, hashmap_iterator_t* iter)
{
    if (NULL == map || NULL == iter)
    {
        return NULL;
    }

    iter->map = map;
    iter->next = NULL;
    iter->next_node = NULL;
    iter->next_index = 0;

    hashmap_node_t* result = NULL;
    AcquireSRWLockExclusive(&map->lock);

    iter->is_old_table = HASHMAP_REHASH == map->status;
    if (0 != iter->is_old_table) iter->next_index = map->move_index;

    result = iter->next_node = hashmap_iterate_find_next(iter);
    if (NULL != result) iter->next_node = hashmap_iterate_find_next(iter);
    
    iter->next = map->iter_head;
    map->iter_head = iter;

    ReleaseSRWLockExclusive(&map->lock);
    return result;
}

// 获取下一个节点
// 使用 hashmap_iterate_begin 初始化过的 iter
hashmap_node_t* hashmap_iterate_next(hashmap_iterator_t* iter)
{
    if (NULL == iter || NULL == iter->map)
    {
        return NULL;
    }

    hashmap_t* map = iter->map;
    AcquireSRWLockShared(&map->lock);

    hashmap_node_t* cur = iter->next_node;
    if (NULL != cur)
    {
        iter->next_node = hashmap_iterate_find_next(iter);
    }

    ReleaseSRWLockShared(&map->lock);
    return cur;
}

// 迭代器结束
void hashmap_iterate_end(hashmap_iterator_t* iter)
{
    if (NULL == iter || NULL == iter->map)
    {
        return;
    }

    hashmap_t* map = iter->map;
    AcquireSRWLockExclusive(&map->lock);

    hashmap_iterator_t* cur = map->iter_head;
    hashmap_iterator_t* prev = NULL;
    while (NULL != cur)
    {
        if (iter == cur)
        {
            if (NULL != prev) prev->next = cur->next;
            else map->iter_head = cur->next;
            break;
        }

        prev = cur;
        cur = cur->next;
    }

    ReleaseSRWLockExclusive(&map->lock);
    iter->map = NULL;
}

// 哈希表大小
size_t hashmap_size(hashmap_t* map)
{
    if (NULL == map) return 0;
    AcquireSRWLockShared(&map->lock);
    size_t size = map->size;
    ReleaseSRWLockShared(&map->lock);
    return size;
}

// 关闭哈希表
void hashmap_close(hashmap_t* map)
{
    if (NULL == map) return;
    if (NULL != map->old_table)
    {
        free(map->old_table);
    }
    
    free(map->table);
    free(map);
}
