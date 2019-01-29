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
  (entry-count [c] "This is lame, wish we had ICounted...")
  (get-entry [c idx])
  (set-entry [c idx v]))

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

(def coltypes 
  '{"DOUBLE"  [DoubleColumn double]
    "LONG"    [LongColumn long]  
    "SHORT"   [ShortColumn short]
    "INTEGER" [IntColumn int]
    "FLOAT"   [FloatColumn float]
    "BOOLEAN" [BooleanColumn boolean]
    "STRING"  [StringColumn str]})
   
(defmacro typed-apply [type col f]
  (let [[coltype coercion] (or (coltypes type)
                               (throw (ex-info "Uknown Column Type"
                                               {:column-type type}))) 
        typed-col (with-meta (gensym "typed-col") {:type  type})]
    `(let [~typed-col ~col
           func#      ~f]
       (parallel/parallel-for idx# (entry-count ~typed-col)
                              (->> (.get ~typed-col idx#)
                                   ~f
                                   ~coercion
                                   (.set ~typed-col idx#))))))

;;wrapper to help us out here...could use macrology to
;;make this simpler....are there a set of base operations
;;we can use to define appropriate wrappers?
(deftype tablesaw-column [^Column col]
  IColumn
  (column-name [c]       (.name col))
  (column-type [c]       (.type col))
  (entry-count [c]       (.count col))
  (get-entry   [c idx]   (.get col (int idx))) 
  (set-entry   [c idx v] (.set col (int idx) v))
  IColumnEntries  ;;maybe revisit? could also cache.
  (entries [c] (vec (.asList col)))
  IUnique
  (-unique-set   [c] (column->unique-set col)) ;;could cache this.
  (-unique-count [c] (column->unique-count col))
  IMissing
  (-missing-count [c] (.countMissing col))
  IColumnOps
  (map-column! [c f]
    ;;todo -> look at coercing this to keys.
    (do   (case (-type-name (column-type col)) 
            "DOUBLE"  (typed-apply DoubleColumn  col f)  
            "LONG"    (typed-apply LongColumn    col f)
            "SHORT"   (typed-apply ShortColumn   col f)
            "INTEGER" (typed-apply IntColumn     col f)
            "FLOAT"   (typed-apply FloatColumn   col f)
            "BOOLEAN" (typed-apply BooleanColumn col f) 
            "STRING"  (typed-apply StringColumn  col f)
            (throw (ex-info "unknown entry-type!" {:entry-type (-type-name (column-type c))
                                                    :column-type (type col)})))
          c))
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

(defn unique [col]
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
   ;;revisit this...might have a better way of
   ;;reporting common types, can eschew tablesaw.
   :type (-> col column-type type-name ->kebab-case)
   :size (entry-count col)
   :num-missing (count-missing col)
   :num-unique  (count-unique col)})


;;deprecated, use protocol fn
#_(defn column-name
  [^Column item]
  (.name item))

;;this may be problematic / unnecessary since
;;it breaks the tablesaw Table type.  Think more.
;;We could implement column-map directly
;;and provide associative access implementations.
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

;;we may need to be able to define conversion, like as-double-column.
;;TODO revisit this and think on it...
(defn column-double-op
  [dataset column-name double-fn]
  (update-column
   dataset column-name
   (fn [^Column col]
     (let [^DoubleColumn double-col (.asDoubleColumn ^NumericColumn col)]
       (map-column! double-col double-fn double-col)
       #_(parallel/parallel-for
        idx (entry-count double-col)
        (.set double-col idx
              (double (double-fn (.getDouble double-col idx)))))
       #_double-col))))

;;conenience function.
(defn log1p
  [col-name dataset]
  (column-double-op dataset col-name #(Math/log (+ 1.0 (double %)))))

;;TODO: looks at generalizing beyond relying on interop and NumericColumn...
(defn numeric? [col]
  (instance? NumericColumn col))

;;TODO are these predicates or function of  :: dataset -> dataset?
(defn numeric-missing?
  [col]
  (and (numeric? col)
       (pos? (count-missing col)))))

(defn non-numeric-missing?
  [col]
  (and (not (numeric? col))
       (pos? (count-missing col))))

(defn col-map
  [map-fn & args]
  (apply map map-fn (map columns args)))

;;TODO: revisit this.  There's probably a common
;;operation happening here, looks like we're trying to
;;efficiently transform the backing string array, probably
;;for labeling purposes...
(defn update-strings
  [str-fn dataset]
  (col-map (fn [col]
             (if (= "string" (:type (column->metadata col)))
                (let [^"[Ljava.lang.String;" str-data (make-array String (.size col))]
                  (parallel/parallel-for
                   idx (entry-count col)
                   (aset str-data idx (str (str-fn (.getString ^StringColumn col idx)))))
                  (StringColumn/create (column-name col) str-data))
                col))
           dataset))

;;operations specific to coalesced dataset coercion?
(def col-type->datatype-map
  {"short" :int16
   "integer" :int32})

(defn col-datatype-cast
  [data-val ^Column column]
  (let [column-dtype-name (-> column
                              column-type ;(.type)
                              type-name  ;(.name)
                              ->kebab-case)]
    (if-let [dtype (get col-type->datatype-map column-dtype-name)]
      (dtype/cast data-val dtype)
      (throw (ex-info "Failed to map numeric datatype to datatype library"
                      {:column-type column-dtype-name})))))

;;This is just filling in holes for missing data with a
;;default value.
(defn update-numeric-missing
  [num-val dataset]
  (col-map (fn [^Column col]
             (if (numeric-missing? col)
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
    (:column retval) ;;seems like it's wrong?
    (throw (ex-info (format "Failed to find column %s" entry-kwd)
                    {:columns (set (keys column-map))}))))

;;unknown what this is intended for...
(defn set-string-table
  ^StringColumn [^StringColumn column str-table]
  (when-not (= (unique column)
               (set (keys str-table)))
    (throw (ex-info "String table keys existing unique set mismatch"
                    {:str-table-keys (set (keys str-table))
                     :column-unique-set (unique column)})))
  (let [new-str-table (StringColumn/create (column-name column))]
    ;;uggh.  Create the appropriate type for the size of the unique-set
    ;;and build the forward/reverse mappings.
    ;;Then set all the values and you should have it.
    )

  )


;;todo - replace with core.matrix?
#_(defprotocol IColumn
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
          (let [retval (get label-entry (get-entry column row-idx))]
            (checknan col-kwd row-idx retval)))
        #(get-entry )

(fn [col-kwd]
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
