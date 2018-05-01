//
//  bPlusTree.hpp
//  b+ Tree
//
//  Created by Tian Zhang on 2/19/18.
//  Copyright © 2018 Tian Zhang. All rights reserved.
//

#ifndef bPlusTree_hpp
#define bPlusTree_hpp

#include <stdio.h>
class Node;

template<class T>
class BPlusTree<T> {
public:
    BPlusTree() {
        
    }
    
    void insertNode(const Node& node);
    void deleteNode(const Node& node);

    bool validBPlusTree();

    
private:
    Node *root;
    int order;

};




#endif /* bPlusTree_hpp */
