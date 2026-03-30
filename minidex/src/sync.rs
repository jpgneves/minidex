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
