use std::sync::Arc;
use rand::Rng;
use rand::rngs::ThreadRng;
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


fn merge<T: PartialOrd + Clone>(list1: &Vec<T>, list2: &Vec<T>) -> Vec<T> {
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

fn parallel_merge_sort<T: PartialOrd + Clone + Send + Sync + 'static>(list: &[T], parallelism_depth: u8) -> Vec<T> {
    let list_copy: Arc<[T]> = Arc::from(list.to_vec().into_boxed_slice());
    _parallel_merge_sort(list_copy, 0, list.len(), parallelism_depth)
}

fn _parallel_merge_sort<T: PartialOrd + Clone + Send + Sync + 'static>(list: Arc<[T]>, begin: usize, end: usize, parallelism_depth: u8) -> Vec<T> {
    if end - begin == 1 {
        list.to_vec()
    } else if parallelism_depth > 0 {
        let pivot: usize = (begin + end) / 2;

        let first_ref = Arc::clone(&list);
        let first_thread = thread::spawn(move || _parallel_merge_sort(first_ref, begin, pivot, parallelism_depth - 1));

        let second_ref = Arc::clone(&list);
        let second_thread = thread::spawn(move || _parallel_merge_sort(second_ref, pivot, end, parallelism_depth - 1));

        merge(&first_thread.join().unwrap(), &second_thread.join().unwrap())
    } else {
        merge_sort(&list[begin..end])
    }
}

fn random_range(rng: &mut ThreadRng, n: usize, lower: usize, upper: usize) -> Vec<usize> {
    (0..n).map(|_| rng.gen_range(lower..upper)).collect::<Vec<usize>>()
}

fn main() {
    let mut rng = rand::thread_rng();
    let list = random_range(&mut rng, 50_000, 0, 100);
    assert!(!is_sorted(&list), "`list` is sorted! This can technically occur by chance, but should be very unlikely if `n` is sufficiently high.");

    let start = Instant::now();
    let parallel_merge_sorted = parallel_merge_sort(&list, 2);
    assert!(is_sorted(&parallel_merge_sorted));
    let duration = start.elapsed();
    println!("Successfully sorted using parallel merge sort in {:#?}!", duration);

    let start = Instant::now();
    let merge_sorted = merge_sort(&list);
    assert!(is_sorted(&merge_sorted));
    let duration = start.elapsed();
    println!("Successfully sorted using merge sort in {:#?}!", duration);

    let start = Instant::now();
    let bubble_sorted = bubble_sort(&list);
    assert!(is_sorted(&bubble_sorted));
    let duration = start.elapsed();
    println!("Successfully sorted using bubble sort in {:?}!", duration);
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
    let sorted_list = parallel_merge_sort(&list, 2);
    assert_eq!(sorted_list, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
}
