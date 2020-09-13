/*
 * Cache逻辑
 * 本Cache封装使用一个特定的全局变量存储缓存数据，因此仅适用于同一Server实例服务内
 * 对于需要跨主机缓存的数据，请使用Memcache实现
 * @Author yorkershi
 * @Create 2017-03-29
 */

package golib

import (
	"fmt"
	"sync"
	"time"
)

type cacheNode struct {
	Data       interface{}
	Expiration time.Time
}

var sysGlobalCacheStack map[string]cacheNode
var mutexSysGlobalCacheStack sync.RWMutex

func init() {
	sysGlobalCacheStack = make(map[string]cacheNode)
	NewCache().Clean()
}

type Cache struct {
}

func NewCache() *Cache {
	return &Cache{}
}

//定时清理过期的缓存数据
func (this *Cache) Clean() {
	//Log("INFO", "Start a daemon goroutine to clean up expired data.")
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println(r)
			}
		}()
		for {
			mutexSysGlobalCacheStack.Lock()
			if len(sysGlobalCacheStack) > 0 {
				for key, node := range sysGlobalCacheStack {
					if time.Now().After(node.Expiration) {
						delete(sysGlobalCacheStack, key)
					}
				}
			}
			mutexSysGlobalCacheStack.Unlock()
			time.Sleep(30 * time.Second)
		}
	}()
}

//设置一个值到cache中
func (this *Cache) Set(key string, value interface{}, expiration time.Duration) {
	//启动一个独立的goroutine，定时清理过期的缓存数据
	//startToCleanCacheOnce.Do(this.clean)
	mutexSysGlobalCacheStack.Lock()
	defer mutexSysGlobalCacheStack.Unlock()
	sysGlobalCacheStack[key] = cacheNode{Data: value, Expiration: time.Now().Add(expiration)}

}

//从cache中获取一个值
func (this *Cache) Get(key string) interface{} {
	//操作全局map，加锁
	mutexSysGlobalCacheStack.RLock()
	v, ok := sysGlobalCacheStack[key]
	mutexSysGlobalCacheStack.RUnlock()
	if !ok {
		return nil
	}

	//检查是否过期
	if time.Now().Before(v.Expiration) {
		return v.Data
	}
	return nil
}

//设置一个字符串到cache中
func (this *Cache) SetString(key, value string, expiration time.Duration) {
	this.Set(key, value, expiration)
}

func (this *Cache) GetString(key string) string {
	v := this.Get(key)
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%s", v)
}

//设置一个int64类型到cache中
func (this *Cache) SetInt(key string, value int64, expiration time.Duration) {
	fmt.Println("get v1.1.5")
	this.Set(key, value, expiration)
}
func (this *Cache) GetInt(key string) int64 {
	v := this.Get(key)
	if v == nil {
		return 0
	}
	if val, ok := v.(int64); ok {
		return val
	} else {
		return 0
	}
}

//设置一个map[string]string内型到cache中
func (this *Cache) SetMap(key string, value map[string]string, expiration time.Duration) {
	this.Set(key, value, expiration)
}
func (this *Cache) GetMap(key string) map[string]string {
	v := this.Get(key)
	if v == nil {
		return nil
	}
	retval, ok := v.(map[string]string)
	if ok {
		return retval
	}
	return nil
}

//设置一个map[string]interface{}内型到cache中
func (this *Cache) SetMapInterface(key string, value map[string]interface{}, expiration time.Duration) {
	this.Set(key, value, expiration)
}
func (this *Cache) GetMapInterface(key string) map[string]interface{} {
	v := this.Get(key)
	if v == nil {
		return nil
	}
	retval, ok := v.(map[string]interface{})
	if ok {
		return retval
	}
	return nil
}

//设置一个[]map[string]interface{}内型到cache中
func (this *Cache) SetMapInterfaceList(key string, value []map[string]interface{}, expiration time.Duration) {
	this.Set(key, value, expiration)
}
func (this *Cache) GetMapInterfaceList(key string) []map[string]interface{} {
	v := this.Get(key)
	if v == nil {
		return nil
	}
	retval, ok := v.([]map[string]interface{})
	if ok {
		return retval
	}
	return nil
}

//设置一个字符串列表到Cache中
func (this *Cache) SetStringList(key string, value []string, expiration time.Duration) {
	this.Set(key, value, expiration)
}
func (this *Cache) GetStringList(key string) []string {
	v := this.Get(key)
	if v == nil {
		return nil
	}
	if val, ok := v.([]string); ok {
		return val
	} else {
		return nil
	}
}
