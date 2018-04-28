//
// Created by Tian Zhang on 4/26/18.
//

#ifndef B_TREE_OBJECT_H
#define B_TREE_OBJECT_H
template <class K, class V>
class KVObject{
public:
    V getValue() {
        return value;
    }

    K getKey() {
        return key;
    }

    void setKeyValue(const K* k, const V* v) {
        this->key = k;
        this->value = v;
    }
private:
    K key;
    V value;

};
#endif //B_TREE_OBJECT_H
