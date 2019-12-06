(ns fntomata.core.alpha
  (:refer-clojure :exclude [dedupe])
  (:require [clojure.core.async :as a]
            [clojure.core.cache :as cache]))

(def ^:dynamic *recv-message* (fn [m]))

(defn send-message [m]
  (*recv-message* {:message-id (java.util.UUID/randomUUID)
                   :message m})
  nil)

(defn combine-recv-messages [recv-messages]
  (let [uniq-fns (into #{} (remove nil? recv-messages))]
    (fn [m]
      (doseq [recv-message uniq-fns]
        (recv-message m)))))

(defn with-deduping-recv-message* [recv-f body-f]
  (let [encountered-ids (atom #{})]
    (binding [*recv-message* (fn [m]
                               ;; Have we already received the message?
                               ;; If not, call recv-f.
                               (let [[old new] (swap-vals! encountered-ids conj (:message-id m))]
                                 (when-not (= old new)
                                   (recv-f (:message m)))))]
      (body-f))))

(defmacro with-deduping-recv-message [f & body]
  `(with-deduping-recv-message* ~f (fn [] ~@body)))

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
    :or {timeout-chan-fn a/timeout}}]
  (let [out (a/chan)
        has-timeout? (and timeout-chan-fn max-age-ms)]
    (assert in "The in channel must be provided")
    (assert (or max-count has-timeout?) "At least one of max-count or max-age-ms must be provided")

    (a/go-loop [buf []
                t nil]
      (let [[v p] (a/alts! (if t [in t] [in]))]
        (cond
          ;; Timed out. Flush the buffer.
          (= p t)
          (do
            (a/>! out buf)
            (recur [] nil))

          ;; `in` is closed. Flush the buffer. Stop processing.
          (nil? v)
          (when (seq buf)
            (a/>! out buf)
            (a/close! out))

          ;; Reached limit (if a limit is set). Flush the buffer.
          (and max-count
               (>= (inc (count buf)) max-count))
          (do
            (assert (= (inc (count buf)) max-count))
            (a/>! out (conj buf v))
            (recur [] nil))

          ;; Add to the buffer. Set a timeout if configured and if none exists yet.
          :else
          (recur (conj buf v)
                 (cond
                   t t
                   has-timeout? (timeout-chan-fn max-age-ms)
                   :else nil)))))
    out))

(defn bind-default
  "Wraps the CPS function to instantly handle exceptions, by sending args that are Throwable to cb."
  [arg cb cps-fn]
  (if (instance? Throwable arg)
    (cb arg)
    (cps-fn arg cb)))

(def ^:dynamic *bind* "A monadic bind function" bind-default)

(defn E
  [cps-fn]
  (fn [arg cb]
    (*bind* arg cb cps-fn)))

(defn route-by-topic [get-topic create-cps-for-topic]
  (let [topic-cps-cache (atom (cache/basic-cache-factory {}))
        get-cps-for-topic (fn [topic]
                            (get (swap! topic-cps-cache cache/through-cache topic (fn [_] (create-cps-for-topic topic)))
                                 topic))]
    (E (fn [arg cb]
         (let [topic (get-topic arg)
               cps-fn (get-cps-for-topic topic)]
           (cps-fn arg cb))))))

(defn cps->async
  "Converts a CPS function, to a function that accepts an argument + returns a channel with the result.
  
  Inverse of async->cps.
  
  Pseudo-signature: (Arg, Result -> ()) -> (Arg -> Channel<Result>)
  "
  [cps-fn]
  (fn [arg]
    (let [c (a/promise-chan)]
      (cps-fn arg (fn [result]
                    (if (nil? result)
                      (a/close! c)
                      (a/put! c result))))
      c)))

(defn async->cps
  "Converts a function that accepts an argument + returns a channel with the result, to a CPS function.
  
  Inverse of cps->async.
  
  Pseudo-signature: (Arg -> Channel<Result>) -> (Arg, Result -> ())
  "
  [async-fn]

  (fn [arg cb]
    (a/go
      (try
        (let [result (a/<! (async-fn arg))]
          (cb result))
        (catch Throwable e
          (cb e))))))

(defn timeout [ms]
  (E (fn [arg cb]
       (a/thread
         (Thread/sleep ms)
         (cb arg)))))

(defn chain
  "Composes CPS functions in the order provided, and returns a new CPS function.
  Useful for avoiding callback hell."

  ([]
   (E (fn [arg cb] (cb arg))))

  ([f1 & rest-cps-fns]
   (E (fn [arg cb] (f1 arg (fn [r] ((apply chain rest-cps-fns) r cb)))))))

(defn- capture-throwable
  "For internal use only.
  Wraps f. If an exception of Throwable is thrown in f, then return the exception instead of throwing it."
  [f]
  (fn [& args]
    (try (apply f args) (catch Throwable e e))))

(defn tap
  "Creates a CPS function that runs f with the argument, and continues the continuation.
  The result of f is unused.
  Useful for side-effects, like for println debugging."
  [f]
  (E (fn [arg cb]
       (cb ((capture-throwable (fn [x] (f x) x)) arg)))))


(defn return
  [f]
  (E (fn [arg cb]
       (cb ((capture-throwable f) arg)))))

(defn batch
  "Once f calls back, cb of the returned CPS function is passed the vector: [arg batch-result]."
  [{:keys [max-age-ms max-count]} f]
  (let [src (a/chan)
        dst (batch-impl {:in src
                         :max-age-ms max-age-ms
                         :max-count max-count})]

    (a/go-loop []
      (when-let [arg-cb-recv-list (a/<! dst)]
        (binding [*recv-message* (combine-recv-messages (map (fn [x] (nth x 2 nil)) arg-cb-recv-list))]
          (a/thread
            (f (mapv first arg-cb-recv-list)
               (fn [batch-result]
                 (if (instance? Throwable batch-result)
                   (doseq [[_ cb recv-message] arg-cb-recv-list]
                     (binding [*recv-message* recv-message]
                       (cb batch-result)))
                   (doseq [[arg cb recv-message] arg-cb-recv-list]
                     (binding [*recv-message* recv-message]
                       (cb [arg batch-result]))))))))
        (recur)))

    (E (fn [arg cb]
         (a/put! src [arg cb *recv-message*])))))

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
      (remove-dedupe* :b))
  ;; => {:a {:status :processing, :queued [:a]}}

  )

(defn run-cps-with* [cps-fn state t arg-cb-recv-list]
  (binding [*recv-message* (combine-recv-messages (map (fn [x] (nth x 2 nil)) arg-cb-recv-list))]
    (cps-fn [t (mapv first arg-cb-recv-list)]
            (fn [result]
              ;; Send the result to all
              (doseq [[_ cb] arg-cb-recv-list]
                (cb result))

              ;; Run the queued ones
              (let [[old _] (swap-vals! state remove-dedupe* t)
                    queued (get-in old [t :queued])]
                (when-not (empty? queued)
                  (run-cps-with* cps-fn state t queued)))))))

(defn dedupe-queueing-as-batch
  "Only allow one message with the same value to be processed at a time.
  If the same message is received during execution, it queues until the previous message is finished.
  
  cps-fn gets called with the arg: [(select-fn message) [message1 message2 message3...]]
  
  Assumes cps-fn performs an idempotent side-effect.
  "
  [select-fn cps-fn]
  (let [state (atom {})]
    (E (fn [arg cb]
         (let [t (select-fn arg)
               new-state (swap! state update-dedupe* t [arg cb *recv-message*])
               t-count (count (get-in new-state [t :queued]))]
           (when (= t-count 0)
             (run-cps-with* cps-fn state t [[arg cb *recv-message*]])))))))

(defn dedupe-queueing [cps-fn]
  (dedupe-queueing-as-batch identity
                            (fn [[arg _] cb]
                              (cps-fn arg cb))))

(defn conj-vec [coll & xs]
  (apply conj (or coll []) xs))

(defn create-message-replayer []
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
  [cps-fn]
  (let [state (atom {})]
    (E (fn [arg cb]
         (let [t arg
               [old-state new-state] (swap-vals! state update t (fn [l] (-> l
                                                                            (update :message-replayer (fn [x] (or x (create-message-replayer))))
                                                                            (update :cbs conj-vec cb))))
               message-replayer (get-in new-state [t :message-replayer])]
           ;; Send existing messages to this function
           (message-replayer :fn *recv-message*)

           (when-not (contains? old-state t)
             ;; not already running. run it!
             (binding [*recv-message* (fn [m] (message-replayer :message m))]
               (cps-fn arg
                       (fn [result]
                         (let [[old-state _] (swap-vals! state dissoc t)]
                           (doseq [cb (get-in old-state [t :cbs])]
                             (cb result))))))))))))

(defn limit-concurrent
  "Only allow a maximum of n messages to be processed at a time."
  [n cps-fn]
  (let [concurrency-chan (a/chan n)
        args-chan (a/chan)]
    (doseq [_ (range n)]
      (a/put! concurrency-chan :unimportant-value))

    (a/go-loop []
      (when (a/<! concurrency-chan)
        (let [[arg cb recv-message] (a/<! args-chan)]
          (binding [*recv-message* recv-message]
            (cps-fn arg (fn [result] (a/put! concurrency-chan :unimportant-value) (cb result))))
          (recur))))
    (E (fn [arg cb]
         (a/put! args-chan [arg cb *recv-message*])))))

(comment
  
  (def myfunc
    '(batch
      (dedupe-queueing ::some-fn))
    )

  (defn perform-it-1 [arg cb]
    ((timeout 1000) arg cb))

  (defn perform-it-2 [arg cb]
    ((timeout 1000) arg (fn [result]
                          ((timeout 1000) result cb))))


  ;; Synchronous println
  (do
    (def out-agent (agent nil))
    (defn out [& more] (send out-agent (fn [_] (apply println more))) nil))

  (def mya
    (route-by-topic :topic (fn [topic]
                             (batch {:max-age-ms 1000 :max-count 3}
                                    (return (fn [args]
                                              (out "BATCHED ARGS FOR" topic args)
                                              (throw (ex-info "oh noes" {}))
                                              "result"))))))

  (let [cb (fn [result] (out "RESULT" (.getMessage result)))]
    (mya {:topic :foo :message "a"} cb)
    (mya {:topic :bar :message "x"} cb)
    (mya {:topic :foo :message "b"} cb)
    (mya {:topic :foo :message "c"} cb)
    (mya {:topic :foo :message "d"} cb)
    (mya {:topic :foo :message "e"} cb))

  (def myaaaa
    (dedupe-attaching (chain (tap (fn [n] (send-message (str "Running... " n))))
                             (timeout 1000)
                             (return (fn [n] (+ n 1))))))

  (with-deduping-recv-message (fn [m] (out m))
    (send-message "Hello")
    (send-message "World"))

  (binding [*recv-message* (fn [m] (out m))]
    (send-message "Hello!"))

  (with-deduping-recv-message (fn [m] (out "LOG" m))
    (let [cb (fn [result] (out "RESULT" result))]
      (myaaaa 123 cb)
      (myaaaa 123 cb)
      (myaaaa 123 cb)
      (myaaaa 456 cb)
      (myaaaa 123 cb)))

  (def build-it (route-by-topic :feature
                                (fn [feature]
                                  (dedupe-attaching
                                   (limit-concurrent 2 (async->cps (fn [arg]
                                                                     (a/go
                                                                       (send-message {:type :starting-build :arg arg})
                                                                       (a/<! (a/timeout 5000))
                                                                       (send-message {:type :finished :arg arg})

                                                                       "RESULT"))))))))

  (with-deduping-recv-message (fn [m] (out "LOG" m))
    (let [cb (fn [result] (out "RESULT" result))]
      (build-it {:feature "blah"} cb)
      (build-it {:feature "blah"} cb)
      (build-it {:feature "foo"} cb)
      (build-it {:feature "blah"} cb)))


  (def mycps (fn [arg cb]
               (a/go
                 (a/<! (a/timeout 5000))

                 (cb (+ arg 5)))))
  (def myasync (cps->async mycps))

  (mycps 42 (fn [result] (println "RESULT" result)))

  (a/go
    (let [a (myasync 5)
          b (myasync 10)
          c (myasync 15)]

      (println "RESULT A" (a/<! a))
      (println "RESULT B" (a/<! b))
      (println "RESULT C" (a/<! c)))))
