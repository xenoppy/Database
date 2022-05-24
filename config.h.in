/**
 * @file config.h.in
 * @brief
 * configure c/c++
 *
 * @author niexw
 * @email niexiaowen@uestc.edu.cn
 */
#ifndef __DB_CONFIG_H__
#define __DB_CONFIG_H__

#if defined(WIN32)
#    if !defined(WIN32_LEAN_AND_MEAN)
#        define WIN32_LEAN_AND_MEAN
#    endif
#    include <SDKDDKVer.h>
#    include <Windows.h>
#    define OCF_WEAK __declspec(selectany) /* 弱声明 */
#    define NO_VTABLE __declspec(novtable) /* 虚函数表 */
#    if defined(EXPORT)                    /* 动态链接库导入导出 */
#        define DLL_EXPORT __declspec(dllexport)
#    else
#        define DLL_EXPORT __declspec(dllimport)
#    endif
#else
#    define S_OK 0    /* 正常返回 */
#    define S_FALSE 1 /* 异常返回 */
#    define OCF_WEAK __attribute__((weak))
#    define DLL_NO_EXPORT                                                      \
        __attribute__((visibility("hidden"))) /* 禁止符号从dll导出 */
#endif

#if defined(__cplusplus)
#    include <cstddef>/* NULL */
#endif                /* __CPLUSPLUS */

#endif /* __DB_CONFIG_H__ */
