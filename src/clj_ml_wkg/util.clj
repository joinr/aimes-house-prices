(ns clj-ml-wkg.util)

;;fixed return type, turns out it didn't matter (learned something new..)
(defn ^double checknan
  [col-kwd row-idx item]
  (let [retval (double item)]
    (when (Double/isNaN retval)
      (throw (ex-info (format "NAN detected in column: %s[%s]" col-kwd row-idx) {})))
    retval))
