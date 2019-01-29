;;Minor refactoring to separate tablesaw
;;wrapping operations, for potential use
;;in an independent library.
(ns clj-ml-wkg.tablesaw
  (:require [clj-ml-wkg.util :refer [checknan]])
  (:import [tech.tablesaw.api Table ColumnType
            NumericColumn DoubleColumn ShortColumn
            LongColumn IntColumn
            StringColumn BooleanColumn]
           [tech.tablesaw.columns Column]
           [tech.tablesaw.io.csv CsvReadOptions]
           [java.util UUID]))

(defprotocol IColumnar
  (columns [this]))

;;maybe dispense with the column-...and use
;;col-, note: these will likely be directly
;;adapted to core.matrix (at least as a compatibility
;;layer of not default)
(defprotocol IColumn
  (column-name [c])
  (column-type [c])
  ;;maybe change to column-size?
  (entry-count [c] "This is lame, wish we had ICounted..."))

;;more to follow...
(defprotocol IDestructiveColumnOps
  (map-column! [col f]))

;;these may be redundant - we have analogues in Counted and
;;Named, but for now we'll astronaut architect a tad...
(defprotocol IColumnType
  (-type-name [this]
    "Allows for columns to declare types for efficiency.")
  (-byte-size [this]
    "Columns types can communicate their entry size, for portability."))

(extend-protocol IColumnType
  ColumnType
  (-type-name [t] (.name t))
  (-byte-size [t] (.byteSize t)))

;;THis is a poor guess...todo - verify
(def ^:const +object-size+ 16)

(defprotocol IColumnEntries
  (entries [c] "Return an ordered collection of entries from the column."))

;;allow foreign types to implement efficient operations
;;with a seq-based fallback option.
(defprotocol IUnique
  (-unique-set [c]
    "column implements efficient operations to compute a set from entries" )
  (-unique-count [c]
    "column implements efficient operations to count unique elements"))

(defprotocol IMissing
  (-missing-count [c]
    "Column implements efficient operations to count missing elements."))

(defn column->unique-set
  [^Column col]
  (->> (.unique col)
       (.asList)
       set))

(defn column->seq
  [^Column col]
  (->> (.asList col)
       seq))

(defn column->unique-count [^Column col]
  (.uniqueCount col))


(defn map-column [col f]
  (parallel/parallel-for idx (entry-count double-col)
    (.set double-col idx
          (double (double-fn (.getDouble double-col idx))))))

(def +types+ {"DOUBLE"  "Double"
              "LONG"    "Long"
              "SHORT"   "Short"
              "INTEGER" ["Integer" "IntColumn"]
              "FLOAT"   "Float"
              "BOOLEAN" "Boolean"
              "STRING"  "String"})

(def cols (for [t types]
            (let [[t ]
                  upped (str (clojure.string/upper-case (subs t 0 1))
                             (clojure.string/lower-case (subs 1 t)))]
              {:getter (symbol (str "get" upped "."))
               :type   (symbol (str upped "Column"))})))

(defmacro for-column [col type f]
  (let [getter (symbol (get getters type "getObject."))]
    (parallel/parallel-for idx (entry-count double-col)
    (.set double-col idx
          (double (double-fn (.getDouble double-col idx)))))))

;;wrapper to help us out here...could use macrology to
;;make this simpler....are there a set of base operations
;;we can use to define appropriate wrappers?
(deftype tablesaw-column [^Column col]
  IColumn
  (column-name [c]     (.name col))
  (column-type [c]     (.type col))
  (entry-count [c]     (.count col))
  IColumnEntries  ;;maybe revisit? could also cache.
  (entries [c] (vec (.asList col)))
  IUnique
  (-unique-set   [c] (column->unique-set col)) ;;could cache this.
  (-unique-count [c] (column->unique-count col))
  IMissing
  (-missing-count [c] (.countMissing col))
  IColumnOps
  (map-column [c f]
    (case (-type-name (column-type c))
      "DOUBLE"
      "SINGLE"
      "LONG"
      "SHORT"
      "INTEGER"
      "FLOAT"
      "BOOLEAN"
      "STRING")
    )
  ;;convenience functions.
  clojure.lang.IFn
  (invoke [this row-index]
    (.get this (int row-index)))
  clojure.lang.Seqable
  (seq [this] (column->seq col)))

;;protocol-derived API
;;todo: 2x that this is a decent default...
(defn type-name [ct]
  (if (extends? IColumnType (type ct))
    (-type-name ct)
    "object"))

(defn type-size [ct]
  (if (extends? IColumnType (type ct))
    (-byte-size ct)
    +object-size+))

(defn count-unique [col]
  (if (extends? IUnique (type col))
    (-unique-count col)
    (-> col entries distinct count)))

(defn count-missing [col]
  (if (extends? protocol IMissing (type col))
    (-missing-count col)
    (->> col entries (filter (complement identity)) count)))

;;todo count-missing-by, missing-by?, missing, etc.

(defn uniques [col]
  (if (extends? IUnique (type col))
    (-unique-set col)
    (-> col entries distinct)))

(defn missing    [col])
(defn missing-by [keyf col])


(defn ^tech.tablesaw.io.csv.CsvReadOptions$Builder
  ->csv-builder [^String path & {:keys [separator header? date-format]}]
  (if separator
    (doto (CsvReadOptions/builder path)
      (.separator separator)
      (.header (boolean header?)))
    (doto (CsvReadOptions/builder path)
      (.header (boolean header?)))))

(defn ->table
  ^Table [path & {:keys [separator quote]}]
  (-> (Table/read)
      (.csv (->csv-builder path :separator separator :header? true))))

;;todo Revisit this.  I eliminated seeming redundant merge.
(defn column->metadata
  [col]
  {:name (column-name col)
   :type (-> col column-type type-name ->kebab-case)
   :size (entry-count col)
   :num-missing (count-missing col)
   :num-unique  (count-unique col)})


;;deprecated
#_(defn column-name
  [^Column item]
  (.name item))

(defn col-seq->map
  [col-seq]
  (->> (columns col-seq)
       (map (juxt column-name identity))
       (into {})))

(defn update-column
  [dataset column-name f & args]
  (let [new-map (apply update (col-seq->map dataset) column-name f args)]
    (->> (columns dataset)
         (map (comp #(get new-map %) column-name)))))

;;given double-col -> getDouble
;;given type -> (.set blah idx (double (fn  ... (.getType))))


(defn column-double-op
  [dataset column-name double-fn]
  (update-column
   dataset column-name
   (fn [^Column col]
     (let [^DoubleColumn double-col (.asDoubleColumn ^NumericColumn col)]
       (parallel/parallel-for
        idx (entry-count double-col)
        (.set double-col idx
              (double (double-fn (.getDouble double-col idx)))))
       double-col))))

(defn log1p
  [col-name dataset]
  (column-double-op dataset col-name #(Math/log (+ 1.0 (double %)))))

(defn numeric-missing?
  [dataset]
  (->> (columns dataset)
       (filter #(and (instance? NumericColumn %)
                     (> (.countMissing ^Column %) 0)))))

(defn non-numeric-missing?
  [dataset]
  (->> (columns dataset)
       (filter #(and (not (instance? NumericColumn %))
                     (> (.countMissing ^Column %) 0)))))

(defn col-map
  [map-fn & args]
  (apply map map-fn (map columns args)))

(defn update-strings
  [str-fn dataset]
  (col-map (fn [^Column col]
             (if (= "string" (:type (column->metadata col)))
                (let [^"[Ljava.lang.String;" str-data (make-array String (.size col))]
                  (parallel/parallel-for
                   idx (.size col)
                   (aset str-data idx (str (str-fn (.getString ^StringColumn col idx)))))
                  (StringColumn/create (.name col) str-data))
                col))
           dataset))

(def col-type->datatype-map
  {"short" :int16
   "integer" :int32})

(defn col-datatype-cast
  [data-val ^Column column]
  (let [column-dtype-name (-> column
                              (.type)
                              (.name)
                              ->kebab-case)]
    (if-let [dtype (get col-type->datatype-map column-dtype-name)]
      (dtype/cast data-val dtype)
      (throw (ex-info "Failed to map numeric datatype to datatype library"
                      {:column-type column-dtype-name})))))

(defn update-numeric-missing
  [num-val dataset]
  (col-map (fn [^Column col]
             (if (and (instance? NumericColumn col)
                      (> (.countMissing col) 0))
               (let [new-col (.copy col)
                     ^ints missing-data (.toArray (.isMissing new-col))]
                 (parallel/parallel-for
                  idx (alength missing-data)
                  (.set new-col
                        (aget missing-data idx)
                        (col-datatype-cast (long num-val) col)))
                 new-col)
               col))
           dataset))

;;tablesaw specific for now...
(defn get-column
  ^Column [column-map entry-kwd]
  (if-let [retval (get column-map entry-kwd)]
    (:column retval)
    (throw (ex-info (format "Failed to find column %s" entry-kwd)
                    {:columns (set (keys column-map))}))))

;;unknown what this is intended for...
(defn set-string-table
  ^StringColumn [^StringColumn column str-table]
  (when-not (= (column->unique-set column)
               (set (keys str-table)))
    (throw (ex-info "String table keys existing unique set mismatch"
                    {:str-table-keys (set (keys str-table))
                     :column-unique-set (column->unique-set column)})))
  (let [new-str-table (StringColumn/create (.name column))]
    ;;uggh.  Create the appropriate type for the size of the unique-set
    ;;and build the forward/reverse mappings.
    ;;Then set all the values and you should have it.
    )

  )


;;todo - replace with core.matrix?
(defprotocol IColumn
  (get-entry [col row-idx]))

(extend-protocol IColumn
  NumericColumn
  (get-entry [column row-idx]
    (checknan
     col-kwd row-idx
     (.getDouble ^NumericColumn column (int row-idx))))
   BooleanColumn
   (get-entry [column row-idx]
     (checknan
      col-kwd row-idx
      (.getDouble ^BooleanColumn column (int row-idx)))))

(defn ->column-getter [label-map]
  (fn [col-kwd]
    (let [column      (get-column column-map col-kwd)
          label-entry (get label-map col-kwd)]
      (if label-entry
        (fn [row-idx]
          (let [retval (get label-entry (.get ^Column column (int row-idx)))]
            (checknan col-kwd row-idx retval)))
        #(get-entry )


(defn ->tech-ml-dataset
  [{:keys [label-map] :as options} dataset]
  (let [column-map (->> (columns dataset)
                        (map (fn [^Column col]
                               (let [col-name (->keyword-name (.name col))]
                                 [col-name (-> (column->metadata col)
                                               (assoc :name col-name
                                                      :column col))])))
                        (into {}))
        label-map (merge (->> (vals column-map)
                              (filter #(= "string" (get % :type)))
                              (map (fn [{:keys [column name]}]
                                     (let [^StringColumn column column]
                                       [name (->> (column->unique-set column)
                                                  (map-indexed #(vector %2 %1))
                                                  (into {}))])))
                              (into {}))
                         ;;Allow users to override string->int mapping
                         label-map)
        column-sizes (->> (columns dataset)
                          (map #(.size ^Column %))
                          distinct)

        _  (when (> 1 (count column-sizes))
             (throw (ex-info "Mismatched column sizes" {:column-sizes column-sizes})))

        feature-names (->> (mapv ->keyword-name feature-names))
        label-name (->keyword-name label-name)
        n-features (count feature-names)
        col-getter-fn (fn [col-kwd]
                        (let [column (saw/get-column column-map col-kwd)
                              label-entry (get label-map col-kwd)]
                          (if label-entry
                            (fn [row-idx]
                              (let [retval (get label-entry (.get ^Column column (int row-idx)))]
                                (checknan col-kwd row-idx retval)))
                            (cond
                              (instance? NumericColumn column)
                              (fn [row-idx]
                                (checknan
                                 col-kwd row-idx
                                 (.getDouble ^NumericColumn column (int row-idx))))
                              (instance? BooleanColumn column)
                              (fn [row-idx]
                                (checknan
                                 col-kwd row-idx
                                 (.getDouble ^BooleanColumn column (int row-idx))))))))
        feature-columns (mapv col-getter-fn feature-names)
        label-column (col-getter-fn label-name)
        options (assoc options :label-map label-map)
        key-ecount-map (->> feature-names
                            (map #(vector % 1))
                            (into {}))]

    (->> (range (first column-sizes))
         (map (fn [row-idx]
                (let [feature-data (double-array n-features)]
                  (c-for [idx 0 (< idx n-features) (inc idx)]
                         (aset feature-data idx (double ((get feature-columns idx) row-idx))))
                  {::dataset/features feature-data
                   ::dataset/label (double-array [(label-column row-idx)])})))
         (dataset/post-process-coalesced-dataset options
                                                 feature-names
                                                 key-ecount-map
                                                 [label-name]))))


;;deprecated for columns
#_(defn columns  [item]
    (if (extends? IColumnar (type item))
      (columns item)
      (seq item)))
