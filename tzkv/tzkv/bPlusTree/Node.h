//
// Created by Tian Zhang on 4/26/18.
//

#ifndef B_TREE_NODE_H
#define B_TREE_NODE_H
enum Role{
    ROOT,
    LEAF,
    INTERNAL
};
class Node {
public:
    Role role;
};
#endif //B_TREE_NODE_H
