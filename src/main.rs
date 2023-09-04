use std::fmt::Debug;


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
    // FIXME: This is the extra-dumb version of bubble sort; consider improving
    for _ in 0..sortable_list.len() {
        for j in 1..sortable_list.len() {
            if sortable_list[j-1] > sortable_list[j] {
                // FIXME: Why are these `.clone()`s necessary?
                let swap = sortable_list[j-1].clone();
                sortable_list[j-1] = sortable_list[j].clone();
                sortable_list[j] = swap;
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
