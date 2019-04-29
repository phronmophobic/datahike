(ns datahike.index
  (:require [datahike.index.hitchhiker-tree :as dih]
            [datahike.index.persistent-set :as dip])
  #?(:clj (:import [hitchhiker.tree.core DataNode IndexNode]
                   [me.tonsky.persistent_sorted_set PersistentSortedSet])))

(defprotocol IIndex
  (-all [index])
  (-seq [index])
  (-count [index])
  (-insert [index datom index-type])
  (-remove [index datom index-type])
  (-slice [index from to index-type])
  (-flush [index backend]))

(extend-type DataNode
  IIndex
  (-all [eavt-tree]
    (dih/-all eavt-tree :eavt))
  (-seq [eavt-tree]
    (dih/-seq eavt-tree :eavt))
  (-count [eavt-tree]
    (dih/-count eavt-tree :eavt))
  (-insert [tree datom index-type]
    (dih/-insert tree datom index-type))
  (-remove [tree datom index-type]
    (dih/-remove tree datom index-type))
  (-slice [tree from to index-type]
    (dih/-slice tree from to index-type))
  (-flush [tree backend]
    (dih/-flush tree backend)))

(extend-type IndexNode
  IIndex
  (-all [eavt-tree]
    (dih/-all eavt-tree :eavt))
  (-seq [eavt-tree]
    (dih/-seq eavt-tree :eavt))
  (-count [eavt-tree]
    (dih/-count eavt-tree :eavt))
  (-insert [tree datom index-type]
    (dih/-insert tree datom index-type))
  (-remove [tree datom index-type]
    (dih/-remove tree datom index-type))
  (-slice [tree from to index-type]
    (dih/-slice tree from to index-type))
  (-flush [tree backend]
    (dih/-flush tree backend)))


(extend-type PersistentSortedSet
  IIndex
  (-all [eavt-set]
    (dip/-all eavt-set))
  (-seq [eavt-set]
    (dip/-seq eavt-set))
  (-count [eavt-set]
    (dip/-count eavt-set))
  (-insert [set datom index-type]
    (dip/-insert set datom index-type))
  (-remove [set datom index-type]
    (dip/-remove set datom index-type))
  (-slice [set from to _]
    (dip/-slice set from to))
  (-flush [set]
    (dip/-flush set)))

(defmulti empty-index
  "Creates empty index"
  {:arglists '([index index-type])}
  (fn [index index-type & opts] index))

(defmethod empty-index ::hitchhiker-tree [_ _]
  (dih/empty-tree))

(defmethod empty-index ::persistent-set [_ index-type]
  (dip/empty-set index-type))


(defmulti init-index
  "Initialize index with datoms"
  {:arglists '([index datoms indexed index-type])}
  (fn [index datoms indexed index-type] index))

(defmethod init-index ::hitchhiker-tree [_ datoms _ index-type]
  (dih/init-tree datoms index-type))

(defmethod init-index ::persistent-set [_ datoms indexed index-type]
  (dip/init-set datoms indexed index-type))
