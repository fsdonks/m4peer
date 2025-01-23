(ns patch
  (:require [taa.capacity-test]))

(in-ns 'hazeldemo.client)
(defn seq!!
  "Returns a (blocking!) lazy sequence read from a channel.  Throws on err values" 
  [c]
  (lazy-seq
   (when-let [v (a/<!! c)]
     (if (instance? Throwable v)
       (throw v)
       (cons v (seq!! c))))))

(defn ufmap
  ([n f coll]
   (let [pending  (atom 0)
         consumed (atom nil)
         res (a/chan n)
         push-result (fn push-result [x]
                       (let [v (u/unpack x)
                             nxt (swap! pending unchecked-dec)]
                         (a/put! res v)
                         (when (and @consumed (zero? nxt))
                           (a/close! res))))
         ins     (a/chan n)
         _    (a/go-loop []
                (if-let [x (a/<! ins)]
                  (let [in (u/pack x)]
                    (swap! pending unchecked-inc) ;;meh
                    (ch/ftask (partial u/packed-call f in)
                              :callback {:on-response push-result
                                         :on-failure  (fn [ex]
                                                        (println [:bombed :closing])
                                                        (a/close! res))})
                    (recur))
                  (reset! consumed true)))
         jobs (a/onto-chan!! ins coll)]
     (seq!! res)))
  ([f coll] (ufmap 100 f coll)))

#_
(client/compile-all!
 '[(in-ns 'hazeldemo.client)
   (defn seq!!
     "Returns a (blocking!) lazy sequence read from a channel.  Throws on err values" 
     [c]
     (lazy-seq
      (when-let [v (a/<!! c)]
        (if (instance? Throwable v)
          (throw v)
          (cons v (seq!! c))))))
   (defn ufmap
     ([n f coll]
      (let [pending  (atom 0)
            consumed (atom nil)
            res (a/chan n)
            push-result (fn push-result [x]
                          (let [v (u/unpack x)
                                nxt (swap! pending unchecked-dec)]
                            (a/put! res v)
                            (when (and @consumed (zero? nxt))
                              (a/close! res))))
            ins     (a/chan n)
            _    (a/go-loop []
                   (if-let [x (a/<! ins)]
                     (let [in (u/pack x)]
                       (swap! pending unchecked-inc) ;;meh
                       (ch/ftask (partial u/packed-call f in)
                                 :callback {:on-response push-result
                                            :on-failure  (fn [ex]
                                                           (println [:bombed :closing])
                                                           (a/close! res))})
                       (recur))
                     (reset! consumed true)))
            jobs (a/onto-chan!! ins coll)]
        (seq!! res)))
     ([f coll] (ufmap 100 f coll)))
   ])

(in-ns 'm4peer.core)

(def ^:dynamic *cluster-logging* false)

(defmacro with-cluster-logging [topic & body]
  `(let [id# (atom nil)]
     (if-not ~'m4peer.core/*cluster-logging*
       (do ~@body)
       (try (let [~'_   (println [:listening-to ~topic])
                  ~'_   (reset! id# (m4peer.core/start-listening ~topic (partial m4peer.core/print-or-die ~topic)))
                  ~'_   (println [:start-logging ~topic])
                  ~'_   (m4peer.core/start-all-logging! ~topic)
                  res#  (do ~@body)
                  ]
              res#)
            #_
            (finally (m4peer.core/send-die ~topic))))))


(def ^:dynamic *part-size* 100)
(defn exec-experiments [xs]
  (case *run-site*
    :local   (marathon.analysis.random/parallel-exec xs)
    :cluster (if (= *part-size* 0)
               (client/fmap marathon.analysis.random/supply-experiment xs) ;;naive
               (client/ufmap  *part-size* marathon.analysis.random/supply-experiment xs))
    (throw (ex-info "unknown *run-site*" {:in *run-site*}))))

;;we hook the default, when using the peer, to use our version of exec-experiments.
(alter-var-root #'marathon.analysis.random/*exec-experiments* (fn [_] exec-experiments))

(in-ns 'taa.capacity)

(defmacro with-runsite [site & body]
  "Conditionsl binding form.  If site evals to :cluster, then
   we require m4peer.core if it hasn't already been required,
   and bind the *run-site* dynvar to change the replication
   runs to be executed on the cluster.

   Note: was unable to do this with binding macro out of the box,
   since it assumes the m4peer.core/*run-site* macro exists already."
  `(if (= ~site :cluster)
     (binding [m4peer.core/*run-site* :cluster]
       (m4peer.core/with-cluster-logging :random-out
         ~@body))
     (marathon.analysis.util/log-to "random-out.txt" ~@body)))

(defn do-taa-runs [in-path {:keys [identifier
                                   resources-root
                                   phases
                                   compo-lengths
                                   reps
                                   lower
                                   lower-rc
                                   upper
                                   upper-rc
                                   threads
                                   include-no-demand
                                   seed
                                   transform-proj
                                   min-distance
                                   conj-proj
                                   run-site] :or
                            {seed random/+default-seed+
                             lower-rc 1 upper-rc 1
                             min-distance 0} :as input-map}]
  (let [proj (a/load-project in-path)
        proj (merge proj conj-proj)  ;;CHANGED
        proj (-> (if transform-proj
               (a/update-proj-tables transform-proj proj)
               proj)
                  ;;we require rc cannibalization modification for taa
                  ;;but maybe this isn't standard for ac-rc random
                  ;;runs yet
                  (random/add-transform random/adjust-cannibals []))
        ;;proj (ensure-truthy-bools proj) ;;clustering fix.
        out-name (str resources-root "results_" identifier)
        results-path (str out-name ".txt")
        risk-path    (str out-name "_risk.xlsx")
        ;;init random-out logging.
        _ (println "Printing status to random-out.txt")]
    (binding [random/*threads* threads]
      (with-runsite run-site
        (->> (random/rand-runs-ac-rc min-distance lower-rc upper-rc
                                     proj :reps reps :phases phases
                                     :lower lower
                                     :upper upper :compo-lengths
                                     compo-lengths
                                     :seed seed)
             (maybe-demand include-no-demand proj reps phases lower upper)
             (spit-results results-path)
             (process-results risk-path input-map))))))


(in-ns 'taa.capacity-test)

(def ^:dynamic *opts*
  {:threads 4
   :reps 10})


(deftest do-taa-test-timed
  (binding [capacity/*testing?* true]
    (let [;;This will run through the taa preprocessing
          input-map (merge input-map *opts*)
          out-path (capacity/preprocess-taa input-map)
          previous-demands (consistent-demand previous-book)
          previous-supply (consistent-supply previous-book)
          [prev-dmd-clean new-dmd-clean] (clean-demand
                                          previous-demands
                                          (consistent-demand
                                           out-path))]
      (is (= prev-dmd-clean new-dmd-clean)
          "After enabling multiple compos forward, if we only
have ac forward, is our demand still the same?.")
      (is (= previous-supply (consistent-supply out-path))
          "After enabling multiple compos forward, is our ac 
supply still the same at least?")
      (testing "Checking if taa capacity analysis runs complete."
        (time (capacity/do-taa-runs out-path input-map))))))

(deftest do-taa-test-multi-timed
  (binding [capacity/*testing?* true]
    (let [;;This will run through the taa preprocessing
          input-map (merge input-map *opts*)
          out-path (capacity/preprocess-taa input-map)
          previous-demands (consistent-demand previous-book)
          previous-supply (consistent-supply previous-book)
          [prev-dmd-clean new-dmd-clean] (clean-demand
                                          previous-demands
                                          (consistent-demand
                                           out-path))]
      (is (= prev-dmd-clean new-dmd-clean)
          "After enabling multiple compos forward, if we only
have ac forward, is our demand still the same?.")
      (is (= previous-supply (consistent-supply out-path))
          "After enabling multiple compos forward, is our ac 
supply still the same at least?")
      (testing "Checking if taa capacity analysis runs complete."
        (time (capacity/do-taa-runs out-path input-map))))))

(deftest cluster-test-timed
  (binding [capacity/*testing?* true]
    (let [;;This will run through the taa preprocessing
          input-map (merge input-map *opts*)
          input-map (assoc input-map :run-site :cluster)
          out-path (capacity/preprocess-taa input-map)
          previous-demands (consistent-demand previous-book)
          previous-supply (consistent-supply previous-book)
          [prev-dmd-clean new-dmd-clean] (clean-demand
                                          previous-demands
                                          (consistent-demand
                                           out-path))]
      (is (= prev-dmd-clean new-dmd-clean)
          "After enabling multiple compos forward, if we only
have ac forward, is our demand still the same?.")
      (is (= previous-supply (consistent-supply out-path))
          "After enabling multiple compos forward, is our ac
supply still the same at least?")
      (testing "Checking if taa capacity analysis runs complete."
        (time (capacity/do-taa-runs out-path input-map))))))


