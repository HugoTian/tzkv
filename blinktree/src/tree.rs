
use std::Box;
use std::cmp::Ordering;

#[derive(Debug)]
struct leaf<T: Ord, U>{
    key: T,
    val: U
}

struct Node<T: Ord>{
    key: T,
    next: Box<TreeNode>
}

struct TreeNode {

}

impl<T> Node<T> {
    fn new(val : T) -> &T {
        Node(val);
    }
}

struct Value {

}

enum BPlusTree{
    Leaf,
    Node
}

impl<T: Ord, U> BPlusTree{
    pub fn new() -> BPlusTree  {

    }

    fn insert(node : &Node)  {
        node.val

    }

    fn find() {

    }


    fn delete() {

    }

    fn findRange() {

    }
}

