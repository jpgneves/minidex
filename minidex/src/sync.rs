#[cfg(feature = "shuttle")]
pub mod shuttle_sync {
    pub use shuttle::sync::*;
    pub mod thread {
        pub use shuttle::thread::*;

        pub trait JoinHandleExt {
            fn is_completed(&self) -> bool;
        }

        impl<T> JoinHandleExt for JoinHandle<T> {
            fn is_completed(&self) -> bool {
                // In Shuttle, we'll return false to simulate a running thread
                false
            }
        }
    }
    pub mod time {
        pub use std::time::*;
    }
}

#[cfg(not(feature = "shuttle"))]
pub mod std_sync {
    pub use std::sync::*;
    pub mod thread {
        pub use std::thread::*;
        pub trait JoinHandleExt {
            fn is_completed(&self) -> bool;
        }
        impl<T> JoinHandleExt for JoinHandle<T> {
            fn is_completed(&self) -> bool {
                self.is_finished()
            }
        }
    }
    pub mod time {
        pub use std::time::*;
    }
}

#[cfg(feature = "shuttle")]
pub use shuttle_sync::*;
#[cfg(not(feature = "shuttle"))]
pub use std_sync::*;

/// The flush thread needs lower throttling, as otherwise
/// it cannot flush entries fast enough to prevent bottlenecking
/// and using the entire buffer in many cases.
pub(crate) fn flush_thread_prio() {
    #[cfg(target_os = "macos")]
    unsafe {
        libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_UTILITY, 0);
    }
    #[cfg(target_os = "windows")]
    {
        use windows_sys::Win32::System::Threading::{
            GetCurrentThread, SetThreadPriority, THREAD_PRIORITY_BELOW_NORMAL,
        };
        unsafe {
            let thread = GetCurrentThread();

            if SetThreadPriority(thread, THREAD_PRIORITY_BELOW_NORMAL) == 0 {
                let err = std::io::Error::last_os_error();
                log::warn!("failed to set below normal thread priority: {err}");
            }
            set_thread_ecoqos(thread);
        }
    }
    #[cfg(target_os = "linux")]
    {
        unsafe {
            // Linux: Set I/O priority to IDLE (Class 3) via syscall
            // SYS_ioprio_set = 251, IOPRIO_WHO_PROCESS = 1
            libc::syscall(
                libc::SYS_ioprio_set,
                1,
                0,
                3 << 13, // IOPRIO_CLASS_IDLE
            );
        }
    }
}

pub(crate) fn lower_thread_io_prio() {
    #[cfg(target_os = "macos")]
    unsafe {
        libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_BACKGROUND, 0);
    }

    #[cfg(target_os = "windows")]
    {
        use windows_sys::Win32::System::Threading::{
            GetCurrentThread, SetThreadPriority, THREAD_MODE_BACKGROUND_BEGIN,
        };
        unsafe {
            let thread = GetCurrentThread();

            if SetThreadPriority(thread, THREAD_MODE_BACKGROUND_BEGIN) == 0 {
                let err = std::io::Error::last_os_error();
                log::warn!("failed to set background thread priority: {err}");
            }
            set_thread_ecoqos(thread);
        }
    }
    #[cfg(target_os = "linux")]
    {
        unsafe {
            // Linux: Set I/O priority to IDLE (Class 3) via syscall
            // SYS_ioprio_set = 251, IOPRIO_WHO_PROCESS = 1
            libc::syscall(
                libc::SYS_ioprio_set,
                1,
                0,
                3 << 13, // IOPRIO_CLASS_IDLE
            );
        }
    }
}

#[cfg(windows)]
fn set_thread_ecoqos(thread: windows_sys::Win32::Foundation::HANDLE) {
    use windows_sys::Win32::System::Threading::{
        SetThreadInformation, THREAD_POWER_THROTTLING_CURRENT_VERSION,
        THREAD_POWER_THROTTLING_EXECUTION_SPEED, THREAD_POWER_THROTTLING_STATE,
        ThreadPowerThrottling,
    };

    let state = THREAD_POWER_THROTTLING_STATE {
        Version: THREAD_POWER_THROTTLING_CURRENT_VERSION,
        ControlMask: THREAD_POWER_THROTTLING_EXECUTION_SPEED,
        StateMask: THREAD_POWER_THROTTLING_EXECUTION_SPEED,
    };
    if unsafe {
        SetThreadInformation(
            thread,
            ThreadPowerThrottling,
            &state as *const _ as *const core::ffi::c_void,
            std::mem::size_of::<THREAD_POWER_THROTTLING_STATE>() as u32,
        )
    } == 0
    {
        let err = std::io::Error::last_os_error();
        log::warn!("failed to set EcoQoS thread state: {err}");
    }
}
