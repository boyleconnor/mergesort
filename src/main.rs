use rand::Rng;
use rand::rngs::ThreadRng;
use rayon;
use rayon::prelude::*;
use std::cmp;
use std::time::Instant;
use std::thread;

fn is_sorted<T: PartialOrd>(list: &Vec<T>) -> bool {
    let mut previous_item_option: Option<&T> = None;
    for item in list {
        if let Some(previous_item) = previous_item_option {
            if *previous_item > *item {
                return false;
            }
        }
        previous_item_option = Some(item);
    }
    true
}

fn merge<T: PartialOrd + Clone>(list1: &[T], list2: &[T], output: &mut [T]) {
    let (mut i, mut j) = (0, 0);
    while i < list1.len() {
        if j == list2.len() || list1[i] <= list2[j] {
            output[i+j] = list1[i].clone();
            i += 1;
        } else {
            output[i+j] = list2[j].clone();
            j += 1;
        }
    }
    while j < list2.len() {
        output[i+j] = list2[j].clone();
        j += 1;
    }
}

fn merge_sort<T: PartialOrd + Clone + Default>(list: &[T]) -> Vec<T> {
    if list.len() == 1 {
        list.to_vec()
    } else {
        let pivot: usize = list.len() / 2;
        let mut output = vec![T::default(); list.len()];
        merge(
            &merge_sort(&list[0..pivot]),
            &merge_sort(&list[pivot..list.len()]),
            &mut output
        );
        output.to_vec()
    }
}


fn thread_merge<T: Default + PartialOrd + Ord + Clone + Send + Sync + 'static>(left: &[T], right: &[T], output: &mut [T], num_threads: u8) {
    if num_threads == 1 || left.len() <= 2 || right.len() <= 2 {
        let (mut i, mut j) = (0, 0);
        while i < left.len() || j < right.len() {
            if j == right.len() || (i < left.len() && left[i] <= right[j]) {
                output[i+j] = left[i].clone();
                i += 1;
            } else {
                output[i+j] = right[j].clone();
                j += 1;
            }
        }
        return;
    }

    let (smaller, bigger) = if left.len() <= right.len() {
        (left, right)
    } else {
        (right, left)
    };
    let i = bigger.len() / 2;
    // FIXME: Switch to .binary_search_by() to allow using floats
    let j = match smaller.binary_search(&bigger[i]) {
        Ok(val) => val,
        Err(val) => val
    };

    let bottom_share = ((i + j)  as f64 / output.len() as f64) * num_threads as f64;
    let bottom_threads = cmp::max(cmp::min(bottom_share.round() as u8, num_threads - 1), 1);

    let (bigger_bottom, bigger_top) = bigger.split_at(i);
    let (smaller_bottom, smaller_top) = smaller.split_at(j);
    let (output_bottom, output_top) = output.split_at_mut(i + j);

    thread::scope(|s| {
        s.spawn(|| {
            thread_merge(smaller_bottom, bigger_bottom,
                          output_bottom, bottom_threads);
        });
        thread_merge(smaller_top, bigger_top, output_top, num_threads - bottom_threads);
    });
}

fn thread_merge_sort<T: Ord + PartialOrd + Clone + Default + Send + Sync + 'static>(list: &[T], num_threads: u8, use_thread_merge: bool) -> Vec<T> {
    if list.len() == 1 {
        list.to_vec()
    } else if num_threads > 1 {
        let pivot: usize = list.len() / 2;
        let (left_half, right_half) = list.split_at(pivot);

        let left_num_threads = num_threads / 2;

        thread::scope(|s| {
            let first_thread = s.spawn(|| thread_merge_sort(left_half, left_num_threads, use_thread_merge));
            let second_half = thread_merge_sort(right_half, num_threads - left_num_threads, use_thread_merge);

            let mut output = vec![T::default(); list.len()];
            if use_thread_merge {
                thread_merge(&first_thread.join().unwrap(), &second_half, &mut output, num_threads)
            } else {
                merge( &first_thread.join().unwrap(), &second_half, &mut output);
            }
            output
        })
    } else {
        merge_sort(&list)
    }
}

fn rayon_merge_sort<T: PartialOrd + Clone + Default + Send + Sync>(list: &[T]) -> Vec<T> {
    if list.len() == 1 {
        list.to_vec()
    } else {
        let pivot= list.len() / 2;
        let (first_half, second_half) = rayon::join(
            || rayon_merge_sort(&list[0..pivot]),
            || rayon_merge_sort(&list[pivot..list.len()])
        );
        let mut output = vec![T::default(); list.len()];
        merge(&first_half, &second_half, &mut output);
        output
    }
}

fn random_range(rng: &mut ThreadRng, n: usize, lower: usize, upper: usize) -> Vec<usize> {
    (0..n).map(|_| rng.gen_range(lower..upper)).collect::<Vec<usize>>()
}

// FIXME: rayon_merge is still way slower than (serial) merge (when used inside of rayon_merge)
fn rayon_merge<T: PartialOrd + Ord + Clone + Send + Sync>(left_half: &[T], right_half: &[T], output: &mut [T]) {
    // Base case:
    if left_half.len() < 2 || right_half.len() < 2 {
        let (mut i, mut j) = (0, 0);
        while i < left_half.len() && j < right_half.len() {
            if left_half[i] <= right_half[j] {
                output[i+j] = left_half[i].clone();
                i += 1;
            } else {
                output[i+j] = right_half[j].clone();
                j += 1;
            }
        }
        for k in i..left_half.len() { output[j + k] = left_half[k].clone(); }
        for k in j..right_half.len() { output[i + k] = right_half[k].clone(); }
        return;
    }

    // Recursive, parallel case
    let (bigger_array, smaller_array) = if left_half.len() >= right_half.len() {
        (left_half, right_half)
    } else {
        (right_half, left_half)
    };

    let i = bigger_array.len() / 2;
    let target = bigger_array[i].clone();
    let j = match smaller_array.binary_search(&target) {
        Ok(val) => val,
        Err(val) => val
    };

    let (output_bottom, output_top) = output.split_at_mut( i + j);
    let (bigger_bottom, bigger_top) = bigger_array.split_at(i);
    let (smaller_bottom, smaller_top) = smaller_array.split_at(j);

    rayon::join(
        || rayon_merge(bigger_bottom, smaller_bottom, output_bottom),
        || rayon_merge(bigger_top, smaller_top, output_top)
    );
}

fn main() {
    let mut rng = rand::thread_rng();
    let list = random_range(&mut rng, 5_000_000, 0, 5_000_000);
    assert!(!is_sorted(&list), "`list` is sorted! This can technically occur by chance, but should be very unlikely if `n` is sufficiently high.");

    let sorted_first_half = rayon_merge_sort(&list[0..list.len() / 2]);
    let sorted_second_half = rayon_merge_sort(&list[list.len() / 2..list.len()]);

    let start = Instant::now();
    let mut rayon_merge_output = vec![0; list.len()];
    rayon_merge(&sorted_first_half, &sorted_second_half, &mut rayon_merge_output);
    let duration = start.elapsed();
    assert!(is_sorted(&rayon_merge_output));
    println!("Successfully rayon-merged in {:#?}!", duration);

    let start = Instant::now();
    let mut thread_merge_output = vec![0; list.len()];
    thread_merge(&sorted_first_half, &sorted_second_half, &mut thread_merge_output, 16);
    let duration = start.elapsed();
    assert!(is_sorted(&thread_merge_output));
    println!("Successfully thread-merged in {:#?}!", duration);

    let start = Instant::now();
    let mut serial_merged = vec![0; list.len()];
    merge(&sorted_first_half, &sorted_second_half, &mut serial_merged);
    let duration = start.elapsed();
    assert!(is_sorted(&serial_merged));
    println!("Successfully serial-merged in {:#?}!", duration);

    let start = Instant::now();
    let thread_merge_sorted = thread_merge_sort(&list, 16, false);
    let duration = start.elapsed();
    assert!(is_sorted(&thread_merge_sorted));
    println!("Successfully sorted using thread merge sort (with serial merge) in {:#?}!", duration);

    let start = Instant::now();
    let thread_merge_sorted = thread_merge_sort(&list, 16, true);
    let duration = start.elapsed();
    assert!(is_sorted(&thread_merge_sorted));
    println!("Successfully sorted using thread merge sort (with thread merge) in {:#?}!", duration);

    let start = Instant::now();
    let rayon_merge_sorted = rayon_merge_sort(&list);
    let duration = start.elapsed();
    assert!(is_sorted(&rayon_merge_sorted));
    println!("Successfully sorted using rayon merge sort in {:#?}!", duration);

    let start = Instant::now();
    let merge_sorted = merge_sort(&list);
    let duration = start.elapsed();
    assert!(is_sorted(&merge_sorted));
    println!("Successfully sorted using merge sort in {:#?}!", duration);
}

#[test]
fn test_is_sorted() {
    let list = vec![2, 3, 10];
    assert!(is_sorted(&list));
    let list = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    assert!(is_sorted(&list));
}

#[test]
fn test_is_not_sorted() {
    let list = vec![3, 2, 10];
    assert!(!is_sorted(&list));
}

#[test]
fn test_merge_sort() {
    let list = vec![2, 5, 10, 3, 4, 1, 6, 9, 8, 7];
    assert!(!is_sorted(&list));
    let sorted_list = merge_sort(&list);
    assert_eq!(sorted_list, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
}

#[test]
fn test_thread_merge_sort_with_thread_merge() {
    let list = vec![2, 5, 10, 3, 4, 1, 6, 9, 8, 7];
    assert!(!is_sorted(&list));
    let sorted_list = thread_merge_sort(&list, 2, true);
    assert_eq!(sorted_list, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
}

#[test]
fn test_thread_merge_sort_with_serial_merge() {
    let list = vec![2, 5, 10, 3, 4, 1, 6, 9, 8, 7];
    assert!(!is_sorted(&list));
    let sorted_list = thread_merge_sort(&list, 2, false);
    assert_eq!(sorted_list, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
}

#[test]
fn test_rayon_merge() {
    let output = &mut [0; 10];
    rayon_merge(&[1, 3, 3, 5, 9, 9], &[1, 2, 4, 5], output);
    assert_eq!(output, &[1, 1, 2, 3, 3, 4, 5, 5, 9, 9]);
}

#[test]
fn test_rayon_merge_sort() {
    let list = vec![2, 5, 10, 3, 4, 1, 6, 9, 8, 7];
    assert!(!is_sorted(&list));
    let sorted_list = rayon_merge_sort(&list);
    assert_eq!(sorted_list, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
}
