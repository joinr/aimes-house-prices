(ns clj-ml-wkg.aimes-house-prices
  (:require [tech.io :as io]
            [camel-snake-kebab.core :refer [->kebab-case]]
            [tech.parallel :as parallel]
            [clojure.core.matrix.macros :refer [c-for]]
            [tech.ml.dataset :as dataset]
            [clojure.set :as c-set]
            [tech.xgboost]
            [tech.smile.regression]
            [tech.svm]
            [tech.ml-base :as ml]
            [tech.ml.utils :as ml-utils]
            [tech.ml.loss :as loss]
            [tech.datatype :as dtype]
            [clj-ml-wkg.tablesaw :as saw]
            [clj-ml-wkg.util :refer [checknan]]
            [oz.core :as oz]
            )
  (:import [tech.tablesaw.api Table ColumnType
            NumericColumn DoubleColumn
            StringColumn BooleanColumn]
           [tech.tablesaw.columns Column]
           [tech.tablesaw.io.csv CsvReadOptions]
           [java.util UUID]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def load-dataset
  (memoize
   #(saw/->table "data/aimes-house-prices/train.csv")))

(defn load-aimes-dataset
  []
  (->> (load-dataset)
       (saw/update-strings #(if (= "" %)
                          "NA"
                          %))
       ;;There are only three columns that are numeric and have missing data.
       ;;Looking at the descriptions, 0 makes more sense than the column-median.
       (saw/update-numeric-missing 0)
       (saw/log1p "SalePrice")))

;;numeric columns that should be string columns
(def categorical-column-names
  #{"MSSubClass" "OverallQual" "OverallCond"})

(defn ->keyword-name
  [^String val]
  (keyword (->kebab-case val)))


(def aimes-column-names #{"PoolQC"
                          "Heating"
                          "TotalBsmtSF"
                          "HouseStyle"
                          "FullBath"
                          "YearRemodAdd"
                          "BsmtCond"
                          "Fence"
                          "Neighborhood"
                          "LotFrontage"
                          "YrSold"
                          "BldgType"
                          "PoolArea"
                          "GarageCars"
                          "BsmtFinSF2"
                          "RoofMatl"
                          "YearBuilt"
                          "GarageQual"
                          "SalePrice"
                          "LowQualFinSF"
                          "GrLivArea"
                          "Alley"
                          "LandSlope"
                          "Electrical"
                          "SaleType"
                          "PavedDrive"
                          "GarageArea"
                          "BsmtFinType2"
                          "Street"
                          "MSSubClass"
                          "WoodDeckSF"
                          "GarageFinish"
                          "ExterQual"
                          "Exterior2nd"
                          "RoofStyle"
                          "Condition1"
                          "KitchenAbvGr"
                          "BsmtFinType1"
                          "MoSold"
                          "Exterior1st"
                          "FireplaceQu"
                          "Fireplaces"
                          "LotConfig"
                          "CentralAir"
                          "GarageType"
                          "3SsnPorch"
                          "MiscFeature"
                          "Foundation"
                          "OverallCond"
                          "LotShape"
                          "BedroomAbvGr"
                          "Condition2"
                          "1stFlrSF"
                          "EnclosedPorch"
                          "MiscVal"
                          "HeatingQC"
                          "KitchenQual"
                          "2ndFlrSF"
                          "GarageCond"
                          "TotRmsAbvGrd"
                          "GarageYrBlt"
                          "BsmtHalfBath"
                          "OpenPorchSF"
                          "BsmtFinSF1"
                          "LandContour"
                          "LotArea"
                          "MasVnrArea"
                          "ScreenPorch"
                          "MasVnrType"
                          "BsmtFullBath"
                          "BsmtUnfSF"
                          "MSZoning"
                          "BsmtQual"
                          "SaleCondition"
                          "ExterCond"
                          "HalfBath"
                          "Utilities"
                          "Id"
                          "BsmtExposure"
                          "Functional"
                          "OverallQual"})


(def feature-names (c-set/difference aimes-column-names #{"Id" "SalePrice"}))
(def label-name "SalePrice")

;; Tech datasets are essentially row store.
;; Coalesced datasets are dense vectors of data
;; There is a tension here between the column store and row store format
;; that is deeper than I was thinking.  For training, row store makes sense
;; but in my experience data coming in from an API is a sequence of maps and ths
;; you really want some abstraction where you can define some pipeline that applies
;; to either.
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


(defn gridsearch-model
  [dataset-name dataset loss-fn opts]
  (let [gs-options (ml/auto-gridsearch-options opts)]
    (println (format "Dataset: %s, Model %s"
                     dataset-name
                     (:model-type opts)))
    (let [gs-start (System/nanoTime)
          {results :retval
           milliseconds :milliseconds}
          (ml-utils/time-section
           (apply ml/gridsearch
                  [gs-options]
                  [::dataset/features]
                  [::dataset/label]
                  loss-fn dataset
                  :gridsearch-depth 75
                  :top-n 20
                  (apply concat (seq opts))))]
      (->> results
           (mapv #(merge %
                         {:gridsearch-time-ms milliseconds
                          :gridsearch-id (UUID/randomUUID)
                          :dataset-name dataset-name}))))))



(defn do-gridsearch
  [base-systems result-name]
  (let [{options :options
         dataset :coalesced-dataset} (->> (load-aimes-dataset)
                                          (->tech-ml-dataset {:range-map {::dataset/features [-1 1]}}))
        keyset (set (keys (first dataset)))
        feature-keys (disj keyset :Purchase)
        results (->>  base-systems
                      (map #(merge options %))
                      (mapcat
                       (partial gridsearch-model
                                result-name
                                dataset
                                loss/rmse))
                      vec)]
    (io/put-nippy! (format
                    "file://%s-results.nippy"
                    (name result-name))
                   results)
    results))


(defn gridsearch-the-things
  []
  (do-gridsearch [{:model-type :xgboost/regression}
                  {:model-type :smile.regression/lasso}
                  {:model-type :smile.regression/ridge}
                  {:model-type :smile.regression/elastic-net}
                  {:model-type :libsvm/regression}]
                 :aimes-initial))

(defn results->accuracy-dataset
  [gridsearch-results]
  (->> gridsearch-results
       (map (fn [{:keys [average-loss options predict-time train-time]}]
              {:average-loss average-loss
               :model-name (str (:model-type options))
               :predict-time predict-time
               :train-time train-time}))))

(defn accuracy-graphs
  [gridsearch-results]
  (->> [:div
        [:h1 "aimes-initial"]
        [:vega-lite {:repeat {:column [:predict-time :train-time]}
                     :spec {:data {:values (results->accuracy-dataset gridsearch-results)}
                            :mark :point
                            :encoding {:y {:field :average-loss
                                           :type :quantitative}
                                       :x {:field {:repeat :column}
                                           :type :quantitative}
                                       :color {:field :model-name
                                               :type :nominal}
                                       :shape {:field :model-name
                                               :type :nominal}}}}]]
       oz/view!))
