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

pub(crate) fn lower_thread_io_prio() {
    #[cfg(target_os = "macos")]
    unsafe {
        libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_BACKGROUND, 0);

        libc::setpriority(libc::PRIO_PROCESS, 0, 10);
    }

    #[cfg(target_os = "windows")]
    {
        use windows_sys::Win32::System::Threading::{
            GetCurrentThread, SetThreadPriority, THREAD_MODE_BACKGROUND_BEGIN,
        };
        unsafe {
            SetThreadPriority(GetCurrentThread(), THREAD_MODE_BACKGROUND_BEGIN as i32);
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
                (3 << 13) | 0, // IOPRIO_CLASS_IDLE
            );
        }
    }
}
