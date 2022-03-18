(ns ^:no-doc datahike.index.persistent-set
  (:require [me.tonsky.persistent-sorted-set :as set]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [clojure.core.async :as async]
            [datahike.datom :as dd]
            [datahike.constants :refer [tx0 txmax]]
            [datahike.index.interface :as di :refer [IIndex]]
            [konserve.core :as k]
            [konserve.serializers :refer [fressian-serializer]]
            [hasch.core :refer [uuid]])
  #?(:clj (:import [datahike.datom Datom]
                   [org.fressian.handlers WriteHandler ReadHandler]
                   [me.tonsky.persistent_sorted_set PersistentSortedSet Loader Leaf])))

(defn index-type->cmp [index-type]
  (case index-type
    :aevt dd/cmp-datoms-aevt
    :avet dd/cmp-datoms-avet
    dd/cmp-datoms-eavt))

(defn index-type->cmp-quick [index-type]
  (case index-type
    :aevt dd/cmp-datoms-aevt-quick
    :avet dd/cmp-datoms-avet-quick
    dd/cmp-datoms-eavt-quick))

(extend-type PersistentSortedSet
  IIndex
  (-all [pset]
    (identity pset))
  (-seq [pset]
    (seq pset))
  (-count [pset]
    (count pset))
  (-insert [pset datom index-type _op-count]
    (if (set/slice pset
                   (dd/datom (.-e datom) (.-a datom) (.-v datom) tx0)
                   (dd/datom (.-e datom) (.-a datom) (.-v datom) txmax))
      set
      (set/conj set datom (index-type->cmp-quick index-type))))
  (-temporal-insert [pset datom index-type _op-count]
    (set/conj pset datom (index-type->cmp-quick index-type)))
  (-upsert [pset datom index-type _op-count]
    (-> (or (when-let [old (first (set/slice pset
                                             (dd/datom (.-e datom) (.-a datom) nil tx0)
                                             (dd/datom (.-e datom) (.-a datom) nil txmax)))]
              (set/disj pset old (index-type->cmp-quick index-type)))
            pset)
        (set/conj datom (index-type->cmp-quick index-type))))
  (-temporal-upsert [pset datom index-type _op-count]
    (-> (or (when-let [old (first (set/slice pset
                                             (dd/datom (.-e datom) (.-a datom) nil tx0)
                                             (dd/datom (.-e datom) (.-a datom) nil txmax)))]
              (set/conj pset (dd/datom (.-e old) (.-a old) (.-v old) (.-tx old) false)
                        (index-type->cmp-quick index-type)))
            pset)
        (set/conj datom (index-type->cmp-quick index-type))))
  (-remove [pset datom index-type _op-count]
    (set/disj pset datom (index-type->cmp-quick index-type)))
  (-slice [pset from to _]
    (set/slice pset from to))
  (-flush [pset _]
    (set! (.-_root pset)
          (set/-flush (.-_root pset)))
    pset)
  (-transient [pset]
    (transient pset))
  (-persistent! [pset]
    (persistent! pset)))

(defn get-loader [konserve-store]
  (proxy [Loader] []
    (load [address]
      (let [children-as-maps (async/<!! (k/get konserve-store address))]
        (into-array Leaf (map #(set/map->node this %) children-as-maps))))
    (store [children]
      (let [children-as-maps (mapv (fn [n] (set/-to-map n)) children)
            address (uuid)]
        (async/<!! (k/assoc konserve-store address children-as-maps))
        address))))

(defmethod di/empty-index :datahike.index/persistent-set [_index-name store index-type _]
  (with-meta (set/sorted-set-by (index-type->cmp index-type) (get-loader store))
    {:index-type index-type}))

(defmethod di/init-index :datahike.index/persistent-set [_index-name store datoms index-type _ {:keys [indexed]}]
  (let [arr (if (= index-type :avet)
              (let [avet-datoms (filter (fn [^Datom d] (contains? indexed (.-a d))) datoms)]
                (to-array avet-datoms))
              (cond-> datoms
                (not (arrays/array? datoms))
                (arrays/into-array)))]
    (arrays/asort arr (index-type->cmp-quick index-type))
    (with-meta (set/from-sorted-array (index-type->cmp index-type) arr (get-loader store))
      {:index-type index-type})))

(defmethod di/add-konserve-handlers :datahike.index/persistent-set [_index-name store]
  (assoc store :serializers {:FressianSerializer (fressian-serializer
                                                  {"datahike.index.PersistentSortedSet" (reify ReadHandler
                                                                                          (read [_ reader _tag _component-count]
                                                                                            (let [{:keys [index-type root-node]} (.readObject reader)
                                                                                                  loader (get-loader store)
                                                                                                  pset (with-meta (set/sorted-set-by (index-type->cmp index-type) loader)
                                                                                                         {:index-type index-type})]
                                                                                              (set! (._root pset)
                                                                                                    (set/map->node loader root-node))
                                                                                              pset)))
                                                   "datahike.db.Datom" (reify ReadHandler
                                                                         (read [_ reader _tag _component-count]
                                                                           (let [datom-vec (.readObject reader)]
                                                                             (dd/datom-from-reader datom-vec))))}
                                                  {PersistentSortedSet
                                                   {"datahike.index.PersistentSortedSet" (reify WriteHandler
                                                                                           (write [_ writer pset]
                                                                                             (.writeTag    writer "datahike.index.PersistentSortedSet" 1)
                                                                                             (.writeObject writer {:root-node (set/-to-map (.-_root pset))
                                                                                                                   :index-type (:index-type (meta pset))})))}
                                                   Datom
                                                   {"datahike.db.Datom" (reify WriteHandler
                                                                          (write [_ writer datom]
                                                                            (.writeTag    writer "datahike.db.Datom" 1)
                                                                            (.writeObject writer (vec (seq datom)))))}})}))

(defmethod di/konserve-backend :datahike.index/persistent-set [_index-name store]
  store)
