;;Minor refactoring to separate tablesaw
;;wrapping operations, for potential use
;;in an independent library.
(ns clj-ml-wkg.tablesaw
  (:require [clj-ml-wkg.util :refer [checknan]])
  (:import [tech.tablesaw.api Table ColumnType
            NumericColumn DoubleColumn
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
  (entry-count [c] "This is lame, wish we had ICounted..."))

;;allow foreign types to implement efficient operations
;;with a seq-based fallback option.
(defprotocol IUnique
  (-unique [c]))


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

;;seems like a protocol function...
(defn ->column-seq
  [item]
  (if #_(instance? Table item)
      (extends? )
    (columns item)
    (seq item)))

;;doesn't appear to be used at the moment.
(defn column->seq
  [^Column col]
  (->> (.asList col)
       seq))

(defn column->unique-set
  [^Column col]
  (->> (.unique col)
       (.asList)
       set))

(defn column->metadata
  [^Column col]
  (let [num-unique (.countUnique col)]
    (merge
     {:name (.name col)
      :type (->kebab-case (.name (.type col)))
      :size (.size col)
      :num-missing (.countMissing col)
      :num-unique num-unique
      })))


(defn column-name
  [^Column item]
  (.name item))

(defn col-seq->map
  [col-seq]
  (->> (->column-seq col-seq)
       (map (juxt column-name identity))
       (into {})))

(defn update-column
  [dataset column-name f & args]
  (let [new-map (apply update (col-seq->map dataset) column-name f args)]
    (->> (->column-seq dataset)
         (map (comp #(get new-map %) #(.name ^Column %))))))

(defn column-double-op
  [dataset column-name double-fn]
  (update-column
   dataset column-name
   (fn [^Column col]
     (let [^DoubleColumn double-col (.asDoubleColumn ^NumericColumn col)]
       (parallel/parallel-for
        idx (.size double-col)
        (.set double-col idx
              (double (double-fn (.getDouble double-col idx)))))
       double-col))))

(defn log1p
  [col-name dataset]
  (column-double-op dataset col-name #(Math/log (+ 1.0 (double %)))))

(defn numeric-missing?
  [dataset]
  (->> (->column-seq dataset)
       (filter #(and (instance? NumericColumn %)
                     (> (.countMissing ^Column %) 0)))))

(defn non-numeric-missing?
  [dataset]
  (->> (->column-seq dataset)
       (filter #(and (not (instance? NumericColumn %))
                     (> (.countMissing ^Column %) 0)))))

(defn col-map
  [map-fn & args]
  (apply map map-fn (map ->column-seq args)))

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
  (let [column-map (->> (->column-seq dataset)
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
        column-sizes (->> (->column-seq dataset)
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
