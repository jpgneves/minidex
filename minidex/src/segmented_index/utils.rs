use memmap2::Mmap;

/// On Unix platforms, this simply sets the `WillNeed` advice
/// on the memory-mapped file so it can be prefetched by the OS.
/// On Windows, we invoke `PrefetchVirtualMemory` directly, as
/// `memmap2::Mmap::advise` is a Unix-only API.
#[cfg(unix)]
pub fn prefetch_memory(mmap: &Mmap) {
    let _ = mmap.advise(memmap2::Advice::WillNeed);
}

/// On Unix platforms, this simply sets the `WillNeed` advice
/// on the memory-mapped file so it can be prefetched by the OS.
/// On Windows, we invoke `PrefetchVirtualMemory` directly, as
/// `memmap2::Mmap::advise` is a Unix-only API.
#[cfg(windows)]
pub fn prefetch_memory(mmap: &Mmap) {
    use std::os::raw::c_void;
    use windows_sys::Win32::System::Memory::{PrefetchVirtualMemory, WIN32_MEMORY_RANGE_ENTRY};
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    if mmap.is_empty() {
        return;
    }

    let entry = WIN32_MEMORY_RANGE_ENTRY {
        VirtualAddress: mmap.as_ptr() as *mut c_void,
        NumberOfBytes: mmap.len(),
    };

    // PrefetchVirtualMemory asks the Windows memory manager to asynchronously
    // pull these pages from the SSD into RAM.
    unsafe {
        let process = GetCurrentProcess();
        PrefetchVirtualMemory(process, 1, &entry, 0);
    }
}
