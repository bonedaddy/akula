use std::collections::VecDeque;

use hashlink::{LinkedHashMap, LinkedHashSet};

fn find_tip<T>(childs_table: LinkedHashMap<T, LinkedHashSet<T>>) -> Vec<T>
where
    T: std::cmp::Eq,
    T: std::hash::Hash,
    T: std::clone::Clone,
    T: Copy,
{
    let possible_tips: LinkedHashSet<T> = childs_table
        .iter()
        .flat_map(|(_, childs)| childs.clone())
        .collect();
    let mut longest_path: Vec<T> = Vec::new();
    let mut longest_path_len: usize = 0;

    for tip in possible_tips {
        let mut path: Vec<T> = vec![tip];
        let mut queue: VecDeque<T> = VecDeque::new();
        queue.push_back(tip);

        while let Some(v) = queue.pop_front() {
            for (parent, childs) in childs_table.iter() {
                if childs.contains(&v) {
                    queue.push_back(*parent);
                    path.push(*parent);
                    break;
                }
            }
        }

        if path.len() >= longest_path_len {
            longest_path_len = path.len();
            longest_path = path;
        }
    }
    longest_path
}

#[test]
fn test_find_tip() {
    let mut childs_table = LinkedHashMap::new();
    childs_table.insert(1, LinkedHashSet::from_iter(vec![2, 3, 4, 5, 6, 7, 8, 9]));
    childs_table.insert(2, LinkedHashSet::from_iter(vec![10, 11, 12, 13, 14, 15]));
    childs_table.insert(3, LinkedHashSet::from_iter(vec![16, 17, 18, 19, 20]));
    childs_table.insert(4, LinkedHashSet::from_iter(vec![21, 22, 23, 24, 25]));
    childs_table.insert(5, LinkedHashSet::from_iter(vec![26, 27, 28, 29, 30]));
    childs_table.insert(6, LinkedHashSet::from_iter(vec![31, 32, 33, 34, 35]));
    childs_table.insert(31, LinkedHashSet::from_iter(vec![36, 37, 38, 39, 40]));
    childs_table.insert(32, LinkedHashSet::from_iter(vec![41, 42, 43, 44, 45]));
    childs_table.insert(33, LinkedHashSet::from_iter(vec![46, 47, 48, 49, 50]));
    childs_table.insert(34, LinkedHashSet::from_iter(vec![51, 52, 53, 54, 55]));
    childs_table.insert(35, LinkedHashSet::from_iter(vec![56, 57, 58, 59, 60]));
    childs_table.insert(57, LinkedHashSet::from_iter(vec![61, 62]));
    childs_table.insert(61, LinkedHashSet::from_iter(vec![63, 64]));
    childs_table.insert(63, LinkedHashSet::from_iter(vec![65, 66, 67]));
    childs_table.insert(67, LinkedHashSet::from_iter(vec![68, 69]));
    childs_table.insert(68, LinkedHashSet::from_iter(vec![70, 71, 72]));
    childs_table.insert(
        70,
        LinkedHashSet::from_iter(vec![73, 74, 75, 76, 78, 79, 77, 80]),
    );
    childs_table.insert(
        73,
        LinkedHashSet::from_iter(vec![81, 82, 83, 84, 85, 86, 87, 88, 89, 90]),
    );
    childs_table.insert(89, LinkedHashSet::from_iter(vec![91]));

    assert_eq!(
        [91, 89, 73, 70, 68, 67, 63, 61, 57, 35, 6, 1],
        find_tip(childs_table)
    );
}
