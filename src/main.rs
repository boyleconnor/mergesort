use std::sync::Arc;
use rand::Rng;
use rand::rngs::ThreadRng;
use rayon;
use rayon::prelude::*;
use std::time::Instant;
use std::thread;
use tokio;
use futures::future::{BoxFuture, FutureExt};

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


fn bubble_sort<T: PartialOrd + Clone>(list: &Vec<T>) -> Vec<T> {
    let mut sortable_list = list.clone();
    let len = sortable_list.len();
    for i in 0..len {
        for j in 1..len-i {
            if sortable_list[j-1] > sortable_list[j] {
                sortable_list.swap(j-1, j);
            }
        }
    }

    sortable_list
}


fn merge<T: PartialOrd + Clone>(list1: &[T], list2: &Vec<T>) -> Vec<T> {
    let mut new_list: Vec<T> = Vec::with_capacity(list1.len() + list2.len());
    let (mut i, mut j) = (0, 0);
    while i < list1.len() {
        if j == list2.len() || list1[i] <= list2[j] {
            new_list.push(list1[i].clone());
            i += 1;
        } else {
            new_list.push(list2[j].clone());
            j += 1;
        }
    }
    while j < list2.len() {
        new_list.push(list2[j].clone());
        j += 1;
    }

    new_list
}

fn merge_sort<T: PartialOrd + Clone>(list: &[T]) -> Vec<T>{
    if list.len() == 1 {
        list.to_vec()
    } else {
        let pivot: usize = list.len() / 2;
        merge(&merge_sort(&list[0..pivot]), &merge_sort(&list[pivot..list.len()]))
    }
}

fn thread_merge_sort<T: PartialOrd + Clone + Send + Sync + 'static>(list: &[T], num_threads: u8) -> Vec<T> {
    let list_copy: Arc<[T]> = Arc::from(list.to_vec().into_boxed_slice());
    _thread_merge_sort(list_copy, 0, list.len(), num_threads)
}

fn _thread_merge_sort<T: PartialOrd + Clone + Send + Sync + 'static>(list: Arc<[T]>, begin: usize, end: usize, num_threads: u8) -> Vec<T> {
    if end - begin == 1 {
        list.to_vec()
    } else if num_threads > 1 {
        let pivot: usize = (begin + end) / 2;
        let left_num_threads = num_threads / 2;

        let first_ref = Arc::clone(&list);
        let first_thread = thread::spawn(move || _thread_merge_sort(first_ref, begin, pivot, left_num_threads));

        let second_half = _thread_merge_sort(list, pivot, end, num_threads - left_num_threads);

        merge(&first_thread.join().unwrap(), &second_half)
    } else {
        merge_sort(&list[begin..end])
    }
}

fn async_merge_sort<T: PartialOrd + Clone + Send + Sync>(list: &[T]) -> BoxFuture<Vec<T>> {
    if list.len() == 1 {
        async move { list.to_vec() }.boxed()
    } else {
        let pivot = list.len() / 2;
        let first_thread = async_merge_sort(&list[0..pivot]);
        let second_thread = async_merge_sort(&list[pivot..list.len()]);

        async move {
            merge(&first_thread.await, &second_thread.await)
        }.boxed()
    }
}

fn rayon_merge_sort<T: PartialOrd + Clone + Send + Sync>(list: &[T]) -> Vec<T> {
    if list.len() == 1 {
        list.to_vec()
    } else {
        let pivot= list.len() / 2;
        let (first_half, second_half) = rayon::join(
            || rayon_merge_sort(&list[0..pivot]),
            || rayon_merge_sort(&list[pivot..list.len()])
        );
        // let mut output_vec: Vec<T> = vec![T::default(); first_half.len() + second_half.len()];
        // rayon_merge(&first_half, &second_half, &mut output_vec);
        // output_vec
        merge(&first_half, &second_half)
    }
}

fn random_range(rng: &mut ThreadRng, n: usize, lower: usize, upper: usize) -> Vec<usize> {
    (0..n).map(|_| rng.gen_range(lower..upper)).collect::<Vec<usize>>()
}

// FIXME: rayon_merge is still way slower than (serial) merge
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

#[tokio::main]
async fn main() {
    let mut rng = rand::thread_rng();
    let list = random_range(&mut rng, 5_000_000, 0, 100);
    assert!(!is_sorted(&list), "`list` is sorted! This can technically occur by chance, but should be very unlikely if `n` is sufficiently high.");

    let sorted_first_half = rayon_merge_sort(&list[0..list.len() / 2]);
    let sorted_second_half = rayon_merge_sort(&list[list.len() / 2..list.len()]);

    let start = Instant::now();
    let mut rayon_merge_output = vec![0; list.len()];
    rayon_merge(&sorted_first_half, &sorted_second_half, &mut rayon_merge_output);
    assert!(is_sorted(&rayon_merge_output));
    let duration = start.elapsed();
    println!("Successfully rayon-merged in {:#?}!", duration);

    let start = Instant::now();
    let serial_merged = merge(&sorted_first_half, &sorted_second_half);
    assert!(is_sorted(&serial_merged));
    let duration = start.elapsed();
    println!("Successfully merged in {:#?}!", duration);

    let start = Instant::now();
    let thread_merge_sorted = thread_merge_sort(&list, 16);
    assert!(is_sorted(&thread_merge_sorted));
    let duration = start.elapsed();
    println!("Successfully sorted using thread merge sort in {:#?}!", duration);

    let start = Instant::now();
    let rayon_merge_sorted = rayon_merge_sort(&list);
    assert!(is_sorted(&rayon_merge_sorted));
    let duration = start.elapsed();
    println!("Successfully sorted using rayon merge sort in {:#?}!", duration);

    let start = Instant::now();
    let async_merge_sorted = async_merge_sort(&list).await;
    assert!(is_sorted(&async_merge_sorted));
    let duration = start.elapsed();
    println!("Successfully sorted using async merge sort in {:#?}!", duration);

    let start = Instant::now();
    let merge_sorted = merge_sort(&list);
    assert!(is_sorted(&merge_sorted));
    let duration = start.elapsed();
    println!("Successfully sorted using merge sort in {:#?}!", duration);

    // let start = Instant::now();
    // let bubble_sorted = bubble_sort(&list);
    // assert!(is_sorted(&bubble_sorted));
    // let duration = start.elapsed();
    // println!("Successfully sorted using bubble sort in {:?}!", duration);
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
fn test_bubble_sort() {
    let list = vec![2, 5, 10, 3, 4, 1, 6, 9, 8, 7];
    assert!(!is_sorted(&list));
    let sorted_list = bubble_sort(&list);
    assert_eq!(sorted_list, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
}

#[test]
fn test_merge_sort() {
    let list = vec![2, 5, 10, 3, 4, 1, 6, 9, 8, 7];
    assert!(!is_sorted(&list));
    let sorted_list = merge_sort(&list);
    assert_eq!(sorted_list, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
}

#[test]
fn test_parallel_merge_sort() {
    let list = vec![2, 5, 10, 3, 4, 1, 6, 9, 8, 7];
    assert!(!is_sorted(&list));
    let sorted_list = thread_merge_sort(&list, 2);
    assert_eq!(sorted_list, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
}

#[test]
fn test_async_merge_sort() {
    let list = vec![2, 5, 10, 3, 4, 1, 6, 9, 8, 7];
    assert!(!is_sorted(&list));
    let sorted_list = tokio_test::block_on(async_merge_sort(&list));
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
