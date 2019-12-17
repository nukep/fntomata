(ns fntomata.core.alpha2
  (:require [clojure.core.async :as async]
            [clojure.core.cache :as cache]))

(defn create-deduping-on-message
  "Creates an on-message function that relays a message of a given ID to `f` no more than once.
   "
  [f]
  (let [encountered-ids (atom #{})]
    (fn [m]
      (if-let [id (::id m)]
        (let [[old new] (swap-vals! encountered-ids conj id)]
          (when-not (= old new)
            (f m)))
        (f m)))))

(defn send-message
  "An opinionated way to send messages to the caller.
  Each message is annotated with a unique ID and a timestamp.
  If a `type `is provided, wrap the message in a map with a key of `type`. `type` should preferrably be a namespaced keyword."
  ([k type message]
   (send-message k {type message}))
  ([k message]
   (if (map? message)
     (when-let [on-message (:on-message k)]
       (on-message (merge message
                          {::id (java.util.UUID/randomUUID)
                           ::timestamp (java.util.Date.)})))
     (println "WARNING: Message not sent. send-message should only receive a map."))))

(defn then [k result] (when-let [f (:then k)] (f result)))

(defn replace-then [k then-fn] (assoc k :then then-fn))

(defn pick-on-message [k] (select-keys k [:on-message]))

(defn bind-error
  "A monadic bind operator that handles Throwables."
  [k ma continue]
  (if (instance? Throwable ma)
    (then k ma)
    (continue k ma)))

(defn bind-error-or-nil
  [k ma continue]
  (if (or (nil? ma) (instance? Throwable ma))
    (then k ma)
    (continue k ma)))

(defn- chain* [[f & rest-fns :as fns] k a bind]
  (f a (if (empty? rest-fns)
         k
         (replace-then k (fn [ma] (bind k ma (fn [k2 a2]
                                               (chain* rest-fns k2 a2 bind))))))))

(defn chain [& opts-with-fns]
  (let [[opts fns]
        (if (map? (first opts-with-fns))
          [(first opts-with-fns) (rest opts-with-fns)]
          [{} opts-with-fns])

        {:keys [bind] :or {bind bind-error}} opts]
    (fn [arg k]
      (chain* fns k arg bind))))

(defn- repeat-n-times* [n f arg k bind]
  (cond (< n 1) (then k arg)
        (= n 1) (f arg k)
        :else   (f arg (replace-then k (fn [ma]
                                         (bind k ma (fn [k a] (repeat-n-times* (dec n) f a k bind))))))))

(defn repeat-n-times [n f {:keys [bind] :or {bind bind-error}}]
  (fn [arg k]
    (repeat-n-times* n f arg k bind)))

(defn route-by-topic [get-topic create-fn-for-topic]
  (let [topic-fn-cache (atom (cache/basic-cache-factory {}))
        get-fn-for-topic (fn [topic]
                           (get (swap! topic-fn-cache cache/through-cache topic (fn [_] (create-fn-for-topic topic)))
                                topic))]
    (fn [arg k]
      (let [topic (get-topic arg)
            f (get-fn-for-topic topic)]
        (f arg k)))))


;; Heavily modified from https://stackoverflow.com/a/33621605
(defn batch-impl
  "Implements a queue with a maximum message count and/or a maximum message age.
   Values from `in` are buffered and sent to `out` as vectors.
  
   Returns a channel that a consumer takes values from (e.g. with <! or <!!) . Taken values are always vectors.
  
   in         - A channel that a producer sends values to (e.g. with >!, >!! or put!)
   max-age-ms - Once the oldest message is these many milliseconds old, flush the buffer.
   max-count  - Only keep these many messages at a time. If the maximum is reached, flush the buffer to `out`.
  
   timeout-chan-fn - Intended for debug/test purposes. If you alternatively have your own way to enforce max-age-ms, you can define it here.
  "
  [{:keys [in max-age-ms max-count timeout-chan-fn]
    :or {timeout-chan-fn async/timeout}}]
  (let [out (async/chan)
        has-timeout? (and timeout-chan-fn max-age-ms)]
    (assert in "The in channel must be provided")
    (assert (or max-count has-timeout?) "At least one of max-count or max-age-ms must be provided")

    (async/go-loop [buf []
                t nil]
      (let [[v p] (async/alts! (if t [in t] [in]))]
        (cond
          ;; Timed out. Flush the buffer.
          (= p t)
          (do
            (async/>! out buf)
            (recur [] nil))

          ;; `in` is closed. Flush the buffer. Stop processing.
          (nil? v)
          (when (seq buf)
            (async/>! out buf)
            (async/close! out))

          ;; Reached limit (if a limit is set). Flush the buffer.
          (and max-count
               (>= (inc (count buf)) max-count))
          (do
            (assert (= (inc (count buf)) max-count))
            (async/>! out (conj buf v))
            (recur [] nil))

          ;; Add to the buffer. Set a timeout if configured and if none exists yet.
          :else
          (recur (conj buf v)
                 (cond
                   t t
                   has-timeout? (timeout-chan-fn max-age-ms)
                   :else nil)))))
    out))

(defn batch
  "Once f calls back, cb of the returned CPS function is passed the vector: [arg batch-result].
   
  The `bind` option is a monadic 'bind' function. If provided, it will define how to continue the computation after a batch result is received. (by default, bind will short-circuit on error values such as Exxceptions.)
  The `return` option is a monadic 'return' function. If provided, it will transform the original [original-arg, batch-result] result.
  Note: `bind` and `return`, if provided, are usually provided together."
  [{:keys [max-age-ms max-count bind return]
    :or {bind   bind-error
         return (fn [a] a)}}
   f]
  (let [src (async/chan)
        dst (batch-impl {:in src
                         :max-age-ms max-age-ms
                         :max-count max-count})]

    (async/go-loop []
      (when-let [a-k-list (async/<! dst)]
        (f (mapv first a-k-list)
           {:on-message  ;; Broadcast the message to all callers
            (fn [m] (doseq [[_ k] a-k-list]
                      (when-let [on-message (:on-message k)]
                        (on-message m))))

            :then
            (fn [ma]
              (doseq [[a k] a-k-list]
                (bind k ma (fn [k2 batch-result] (then k2 (return [a batch-result]))))))})
        (recur)))

    (fn [a k]
      (async/put! src [a k]))))


(defn update-dedupe* [p t arg]
  (let [status (get-in p [t :status])]
    (cond
      (= status nil)
      (assoc p t {:status :processing :queued []})

      (= status :processing)
      (update-in p [t :queued] conj arg)

      :else
      p)))

(defn remove-dedupe* [p t]
  (if (empty? (get-in p [t :queued]))
    (dissoc p t)
    (assoc-in p [t :queued] [])))

(comment
  (-> {}
      (update-dedupe* :a :a))
  ;; => {:a {:status :processing, :queued []}}

  (-> {}
      (update-dedupe* :a :a)
      (update-dedupe* :a :a))
  ;; => {:a {:status :processing, :queued [:a]}}

  (-> {}
      (update-dedupe* :a :a)
      (update-dedupe* :a :a)
      (update-dedupe* :a :a)
      (update-dedupe* :b :b))
  ;; => {:a {:status :processing, :queued [:a :a]}, :b {:status :processing, :queued []}}

  (-> {:a {:status :processing, :queued [:a]}}
      (remove-dedupe* :a))
  ;; => {:a {:status :processing, :queued []}}

  (-> {:a {:status :processing, :queued []}}
      (remove-dedupe* :a))
  ;; => {}

  (-> {:a {:status :processing, :queued [:a]}}
      (remove-dedupe* :b)))
  ;; => {:a {:status :processing, :queued [:a]}}

(defn dedupe-queueing-as-batch* [f state t a-k-list]
  (f [t (mapv first a-k-list)]
     {:on-message (fn [m] (doseq [[_ k] a-k-list]
                            (when-let [on-message (:on-message k)]
                              (on-message m))))
      :then (fn [ma]
              ;; Send the result to all
              (doseq [[_ k] a-k-list]
                (then k ma))

              ;; Run the queued ones
              (let [[old _] (swap-vals! state remove-dedupe* t)
                    queued (get-in old [t :queued])]
                (when-not (empty? queued)
                  (dedupe-queueing-as-batch* f state t queued))))}))

(defn dedupe-queueing-as-batch
  "Only allow one message with the same value to be processed at a time.
  If the same message is received during execution, it queues until the previous message is finished.
  
  f gets called with the arg: [(select-fn message) [message1 message2 message3...]]
  
  Assumes f performs an idempotent side-effect."
  [select-fn f]
  (let [state (atom {})]
    (fn [arg k]
      (let [t (select-fn arg)
            new-state (swap! state update-dedupe* t [arg k])
            t-count (count (get-in new-state [t :queued]))]
        (when (= t-count 0)
          (dedupe-queueing-as-batch* f state t [[arg k]]))))))

(defn dedupe-queueing [f]
  (dedupe-queueing-as-batch identity
                            (fn [[arg _] k]
                              (f arg k))))

(defn conj-vec [coll & xs]
  (apply conj (or coll []) xs))

(defn- create-message-replayer []
  (let [a (agent {:fns #{} :messages []})]
    (fn [type value]
      (case type
        :fn      (send a (fn [p]
                           ;; Only resend messages if function is new
                           (when-not (contains? (:fns p) value)
                             (doseq [message (:messages p)]
                               (value message)))
                           (update p :fns conj value)))
        :message (send a (fn [p]
                           ;; Send message to all functions
                           (doseq [f (:fns p)]
                             (f value))
                           (update p :messages conj value))))
      nil)))

(defn dedupe-attaching
  "Only allow one message with the same value to be processed at a time.
  If the same message is received during execution, it 'attaches' itself to that existing execution.
  When a message 'attaches', all logged messages get resent to new callers."
  [f]
  (let [state (atom {})]
    (fn [arg k]
      (let [t arg
            [old-state new-state] (swap-vals! state update t (fn [l] (-> l
                                                                         (update :message-replayer (fn [x] (or x (create-message-replayer))))
                                                                         (update :ks conj-vec k))))
            message-replayer (get-in new-state [t :message-replayer])]
        ;; Send existing messages to this function
        (when-let [on-message (:on-message k)]
          (message-replayer :fn on-message))

        (if (contains? old-state t)
          (do
            (send-message k ::dedupe-attaching {:op :already-running :a arg}))

          (do
            ;; not already running. run it!
            (send-message k ::dedupe-attaching {:op :run :a arg})
            (f arg
               {:on-message (fn [m] (message-replayer :message m))
                :then (fn [ma] (let [[old-state _] (swap-vals! state dissoc t)]
                                 (doseq [k (get-in old-state [t :ks])]
                                   (then k ma))))})))))))

(defn limit-concurrent
  "Only allow a maximum of n messages to be processed at a time."
  [n f]
  (let [concurrency-chan (async/chan n)
        a-k-chan (async/chan)]
    (doseq [_ (range n)]
      (async/put! concurrency-chan :unimportant-value))

    (async/go-loop []
      (when (async/<! concurrency-chan)
        (let [[arg k] (async/<! a-k-chan)]
          (f arg (replace-then k (fn [ma] (async/put! concurrency-chan :unimportant-value) (then k ma))))
          (recur))))
    (fn [arg k]
      (async/put! a-k-chan [arg k]))))

(defmacro go-fn [[a-sym k-sym] & body]
  `(fn [~a-sym ~k-sym]
     (async/go
       (then ~k-sym (try
                      ;; Don't allow the go block to access the :then function.
                      ;; Calling it more than once might cause unexpected bugs.
                      (let [~k-sym (dissoc ~k-sym :then)]
                        ~@body)
                      (catch Throwable e# e#))))))

(defmacro <! [c]
  `(let [v# (async/<! ~c)]
     (if (instance? Throwable v#)
       (throw v#)
       v#)))

(defn into-async [f]
  (letfn [(fr
            ([a] (fr a {}))
            ([a k]
             (let [c (async/promise-chan)]
               (f a (assoc k :then (fn [result] (if (nil? result)
                                                  (async/close! c)
                                                  (async/put! c result)))))
               c)))]
    fr))

(defn into-blocking [f]
  (letfn [(fr
            ([a] (fr a {}))
            ([a k]
             (let [c (async/promise-chan)]
               (f a (assoc k :then (fn [result] (if (nil? result)
                                                  (async/close! c)
                                                  (async/put! c result)))))
               (let [result (async/<!! c)]
                 (if (instance? Throwable result)
                   (throw result)
                   result)))))]
    fr))

(comment
  ((chain {:bind bind-list}
          (fn [a k] (then k a))   ;; pass list as-is (like the "return" operator in Haskell)
          (make-dup)
          (make-map (fn [x] (* x 3)))
          (make-map (fn [x] (+ x 1)))
          (fn [a k] (send-message k ::foobar {:message "hello!" :a a}) (then k [a]))
          #_(make-filter even?))
   [1 2 3 4 5]
   {:on-message (fn [m] (println "Message:" m))
    :then (fn [result] (println "Result:" result))})

  ((chain #_(fn [a k] (then k (ex-info "Crash" {})))
          (go
            [a k]
            (send-message k ::blah {:message "Start"})
            (<! (async/timeout 2000))
            (send-message k ::blah {:message "End"})
            (* a 2))
          (fn [a k] (then k (+ a 1))))
   50
   {:on-message (fn [m] (println "Message:" m))
    :then (fn [result] (println "Result:" result))})

  (do *e)

  ;; 2 4 6 8
  ;; 3 5 7 9

  (my-fn-list 2 {:then (fn [result] (println "Result:" result))})

  ((repeat-n-times 3 my-fn-list {:bind bind-list})
   2
   {:then (fn [result] (println "Result:" result))})

  ((repeat-n-times 4 my-fn {:bind bind-experiment})
   10
   {:on-message (fn [m] (println "Message:" m))
    :then (fn [result] (println "Result:" result))})

  ((repeat-n-times 4 my-fn-nil {:bind bind-error-or-nil})
   10
   {:on-message (fn [m] (println "Message:" m))
    :then (fn [result] (println "Result:" result))}))
