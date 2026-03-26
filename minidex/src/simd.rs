#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

/// Intersects two sorted u32 arrays, pushes matches to `out`
pub fn intersect_arrays(a: &[u32], b: &[u32], out: &mut Vec<u32>) {
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            unsafe {
                intersect_neon(a, b, out);
                return;
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe {
                intersect_avx2(a, b, out);
                return;
            }
        }
    }

    intersect_scalar(a, b, out);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn intersect_avx2(a: &[u32], b: &[u32], out: &mut Vec<u32>) {
    let len_a = a.len();
    let len_b = b.len();
    let mut i = 0;
    let mut j = 0;

    // SIMD Galloping 8 items at a time
    while i + 8 <= len_a && j + 8 <= len_b {
        // Because the arrays are sorted, we can check the boundaries of the
        // 8-item chunks without using vector instructions
        let a_min = unsafe { *a.get_unchecked(i) };
        let a_max = unsafe { *a.get_unchecked(i + 7) };
        let b_min = unsafe { *b.get_unchecked(j) };
        let b_max = unsafe { *b.get_unchecked(j + 7) };

        // Skip an entire 256-bit chunk in A if less than B
        if a_max < b_min {
            i += 8;
            continue;
        }

        // Skip an entire 256-bit chunk in B if less than A
        if b_max < a_min {
            j += 8;
            continue;
        }

        // The chunks overlap now, so we fall back to the same scaler intersection
        let end_a = (i + 8).min(len_a);
        let end_b = (j + 8).min(len_b);

        _intersect_scalar(&mut i, a, end_a, &mut j, b, end_b, out);
    }

    intersect_scalar(&a[i..], &b[j..], out);
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn intersect_neon(a: &[u32], b: &[u32], out: &mut Vec<u32>) {
    let len_a = a.len();
    let len_b = b.len();

    let mut i = 0;
    let mut j = 0;

    // SIMD Galloping 4 items at a time
    while i + 4 <= len_a && j + 4 <= len_b {
        let a_ptr = unsafe { a.as_ptr().add(i) };
        let b_ptr = unsafe { b.as_ptr().add(j) };

        // Load 128 bits (4x u32) into NEON 128-bit registers
        let va = unsafe { vld1q_u32(a_ptr) };
        let vb = unsafe { vld1q_u32(b_ptr) };

        // Extract chunk boundaries
        let a_min = vgetq_lane_u32::<0>(va);
        let a_max = vgetq_lane_u32::<3>(va);
        let b_min = vgetq_lane_u32::<0>(vb);
        let b_max = vgetq_lane_u32::<3>(vb);

        // Skip A items if their highest is less than lowest in B
        if a_max < b_min {
            i += 4;
            continue;
        }

        // Skip B items if their highest is less than lowest in A
        if b_max < a_min {
            j += 4;
            continue;
        }

        // The chunks overlap now, so we do a scalar intersection
        let end_a = (i + 4).min(len_a);
        let end_b = (j + 4).min(len_b);

        _intersect_scalar(&mut i, a, end_a, &mut j, b, end_b, out);
    }

    // Remaining items get scalar intersection
    intersect_scalar(&a[i..], &b[j..], out);
}

fn intersect_scalar(a: &[u32], b: &[u32], out: &mut Vec<u32>) {
    let mut i = 0;
    let mut j = 0;

    _intersect_scalar(&mut i, a, a.len(), &mut j, b, b.len(), out);
}

#[inline(always)]
fn _intersect_scalar(
    i: &mut usize,
    a: &[u32],
    a_len: usize,
    j: &mut usize,
    b: &[u32],
    b_len: usize,
    out: &mut Vec<u32>,
) {
    while *i < a_len && *j < b_len {
        // get_unchecked to remove bounds checking overhead
        let val_a = unsafe { *a.get_unchecked(*i) };
        let val_b = unsafe { *b.get_unchecked(*j) };

        if val_a == val_b {
            out.push(val_a);
            *i += 1;
            *j += 1;
        } else if val_a < val_b {
            *i += 1;
        } else {
            *j += 1;
        }
    }
}
