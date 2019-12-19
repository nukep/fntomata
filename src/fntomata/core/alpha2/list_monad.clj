(ns fntomata.core.alpha2.list-monad
  (:refer-clojure :exclude [map filter])
  (:require [fntomata.core.alpha2 :as q]))

(defn bind-list
  "Implemention of the List monad's bind operator.
  Mostly a proof-of-concept. Probably not very useful in practice.
  Short-circuits on Throwable. Errors are racing - subsequent errors are ignored."
  [k ma continue]
  (if (instance? Throwable ma)
    (q/then k ma)
    
    (let [results (atom {})
          failed (atom false)]
      (doseq [[i a] (map-indexed vector ma)]
        (let [new-k (q/replace-then k (fn [ma-result]
                                        (cond
                                          ;; Immediately fail on the first error.
                                          ;; Note: We don't need to set a flag to prevent the successful q/then, because the `results` atom is never completed in this case.
                                          (instance? Throwable ma-result)
                                          (let [[already-failed _] (reset-vals! failed true)]
                                            (when-not already-failed
                                              (q/then k ma-result)))

                                          :else
                                          (let [r (or ma-result [])]
                                            (when-not (coll? r)
                                              (println "WARNING: bind-list received result that doesn't satisfy (coll?):" r))
                                            (let [new (swap! results assoc i r)]
                                              (when (= (count new) (count ma))
                                                ;; All results have arrived.
                                                ;; Flatten the results into a vector and return it.
                                                (q/then k (apply concat (clojure.core/map new (range (count new)))))))))))]
          (continue new-k a))))))

(defn return-list [a]
  [a])

(defn map    [f] (fn [a k] (q/then k [(f a)])))
(defn filter [f] (fn [a k] (q/then k (if (f a) [a] []))))
(defn dup    []  (fn [a k] (q/then k [a a])))