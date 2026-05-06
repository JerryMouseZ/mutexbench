#pragma once

#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <iostream>
#include <string>

#include "timeslice_extension.hpp"

namespace locks_bench {

class McsTasAccordinDirectLibrary {
public:
  using MutexPtr = void *;
  using CreateFn = MutexPtr (*)();
  using DestroyFn = int (*)(MutexPtr);
  using LockFn = int (*)(MutexPtr);
  using UnlockFn = int (*)(MutexPtr);

  static const McsTasAccordinDirectLibrary &instance() {
    static const McsTasAccordinDirectLibrary library;
    return library;
  }

  MutexPtr create() const { return create_(); }
  int destroy(MutexPtr mutex) const { return destroy_(mutex); }
  int lock(MutexPtr mutex) const { return lock_(mutex); }
  int unlock(MutexPtr mutex) const { return unlock_(mutex); }

  [[noreturn]] static void fail(const std::string &message) {
    std::cerr << "mcs_tas_accordin_direct: " << message << "\n";
    std::exit(1);
  }

private:
  McsTasAccordinDirectLibrary()
      : handle_(open_library()),
        create_(load_symbol<CreateFn>("mcs_tas_accordin_direct_mutex_create")),
        destroy_(load_symbol<DestroyFn>(
            "mcs_tas_accordin_direct_mutex_destroy")),
        lock_(load_symbol<LockFn>("mcs_tas_accordin_direct_mutex_lock")),
        unlock_(load_symbol<UnlockFn>(
            "mcs_tas_accordin_direct_mutex_unlock")) {}

  static void *open_library() {
    const char *lib_path = std::getenv("MCS_TAS_ACCORDIN_DIRECT_LIB");
    if (lib_path == nullptr || lib_path[0] == '\0') {
      fail("MCS_TAS_ACCORDIN_DIRECT_LIB is not set");
    }

    void *handle = dlopen(lib_path, RTLD_NOW | RTLD_LOCAL);
    if (handle == nullptr) {
      const char *error = dlerror();
      fail(std::string("failed to dlopen ") + lib_path + ": " +
           (error == nullptr ? "unknown error" : error));
    }
    return handle;
  }

  template <typename Fn> Fn load_symbol(const char *name) const {
    dlerror();
    void *symbol = dlsym(handle_, name);
    const char *error = dlerror();
    if (error != nullptr || symbol == nullptr) {
      fail(std::string("failed to resolve ") + name + ": " +
           (error == nullptr ? "symbol not found" : error));
    }
    return reinterpret_cast<Fn>(symbol);
  }

  void *handle_;
  CreateFn create_;
  DestroyFn destroy_;
  LockFn lock_;
  UnlockFn unlock_;
};

struct McsTasAccordinDirectLockBench {
  struct GuardState {};

  explicit McsTasAccordinDirectLockBench(const LockBenchOptions &options = {})
      : lib_(McsTasAccordinDirectLibrary::instance()),
        timeslice_(options.timeslice_extension_mode), mutex_(lib_.create()) {
    if (mutex_ == nullptr) {
      McsTasAccordinDirectLibrary::fail("mutex_create returned null");
    }
  }

  McsTasAccordinDirectLockBench(const McsTasAccordinDirectLockBench &) =
      delete;
  McsTasAccordinDirectLockBench &
  operator=(const McsTasAccordinDirectLockBench &) = delete;

  ~McsTasAccordinDirectLockBench() {
    if (mutex_ != nullptr) {
      const int ret = lib_.destroy(mutex_);
      if (ret != 0) {
        std::cerr << "mcs_tas_accordin_direct: mutex_destroy failed: ret="
                  << ret << " (" << std::strerror(ret) << ")\n";
      }
    }
  }

  void prepare_thread() { timeslice_.prepare_thread(); }

  [[nodiscard]] GuardState lock() {
    check_ret(lib_.lock(mutex_), "mutex_lock");
    timeslice_.on_critical_section_enter();
    return {};
  }

  void unlock(GuardState &) {
    check_ret(lib_.unlock(mutex_), "mutex_unlock");
    timeslice_.on_critical_section_exit();
  }

private:
  static void check_ret(int ret, const char *operation) {
    if (ret == 0) {
      return;
    }
    McsTasAccordinDirectLibrary::fail(std::string(operation) +
                                      " failed: ret=" + std::to_string(ret) +
                                      " (" + std::strerror(ret) + ")");
  }

  const McsTasAccordinDirectLibrary &lib_;
  CriticalSectionTimesliceExtension timeslice_;
  McsTasAccordinDirectLibrary::MutexPtr mutex_;
};

} // namespace locks_bench
