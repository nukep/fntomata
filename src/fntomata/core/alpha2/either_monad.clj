(ns fntomata.core.alpha2.either-monad
  (:require [fntomata.core.alpha2 :as q]))

(defn bind-either
  "Implemention of the Either monad's bind operator.
  Mostly a proof-of-concept. Probably not very useful in practice.
  
  Note: Left's boxed value can be a string, in the event of unexpected errors.
   
  Converts Throwable value to a left value."
  [k ma continue]
  (if (instance? Throwable ma)
    (q/then k [:left (.getMessage ma)])

    (let [[type value] ma]
      (case type
        :left  (q/then k ma)
        :right (continue k value)
        (q/then k [:left "Error - invalid Either value"])))))

(defn return-either [a]
  [:right a])