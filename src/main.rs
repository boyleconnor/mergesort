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


fn zip<T: PartialOrd + Clone>(list1: &Vec<T>, list2: &Vec<T>) -> Vec<T> {
    let mut new_list: Vec<T> = vec![];
    let mut i = 0;
    let mut j = 0;
    while i < list1.len() || j < list2.len() {
        if i == list1.len() {
            new_list.push(list2[j].clone());
            j += 1;
        } else if j == list2.len() {
            new_list.push(list1[i].clone());
            i += 1;
        } else if list2[j] > list1[i] {
            new_list.push(list1[i].clone());
            i += 1;
        } else {
            new_list.push(list2[j].clone());
            j += 1;
        }
    }

    new_list
}

fn merge_sort<T: PartialOrd + Clone>(list: &Vec<T>) -> Vec<T>{
    if list.len() == 1 {
        list.clone()
    } else {
        let pivot: usize = list.len() / 2;
        zip(&merge_sort(&list[0..pivot].to_vec()), &merge_sort(&list[pivot..list.len()].to_vec()))
    }
}

fn parallel_merge_sort<T: PartialOrd + Clone + Send + 'static>(list: Vec<T>, parallelism_depth: u8) -> Vec<T> {
    if list.len() == 1 {
        list
    } else if parallelism_depth > 0 {
        let pivot: usize = list.len() / 2;
        let first_half = list[0..pivot].to_vec();
        let second_half = list[pivot..list.len()].to_vec();
        let first_thread = thread::spawn(move || parallel_merge_sort(first_half, parallelism_depth - 1));
        let second_thread = thread::spawn(move || parallel_merge_sort(second_half, parallelism_depth - 1));
        zip(&first_thread.join().unwrap(), &second_thread.join().unwrap())
    } else {
        merge_sort(&list)
    }
}

fn random_range(rng: &mut ThreadRng, n: usize, lower: usize, upper: usize) -> Vec<usize> {
    (0..n).map(|_| rng.gen_range(lower..upper)).collect::<Vec<usize>>()
}

fn main() {
    let mut rng = rand::thread_rng();
    let list = random_range(&mut rng, 10_000, 0, 100);
    assert!(!is_sorted(&list), "`list` is sorted! This can technically occur by chance, but should be very unlikely if `n` is sufficiently high.");

    let start = Instant::now();
    let parallel_merge_sorted = parallel_merge_sort(list.clone(), 2);
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
    assert!(is_sorted(&sorted_list));
}

#[test]
fn test_merge_sort() {
    let list = vec![2, 5, 10, 3, 4, 1, 6, 9, 8, 7];
    assert!(!is_sorted(&list));
    let sorted_list = merge_sort(&list);
    assert!(is_sorted(&sorted_list));
}
